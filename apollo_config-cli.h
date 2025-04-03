#pragma once

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "cirtual_breaker.h"

namespace apollo {
namespace client {

using json = nlohmann::json;

class apollo_client {
 public:
  using error_callback = std::function<void(std::string_view message)>;
  using update_callback = std::function<void(std::string_view key, std::string_view value)>;

 private:
  std::string _address;
  std::string _app_id = "default";
  std::string _cluster = "default";
  std::string _request_ip;
  error_callback _error_callback = [](std::string_view message) {};

  util::circuit_breaker _breaker;

  struct namespace_data {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, std::string> cache_configs;

    // long poll using data
    int64_t notification_id = -1;
    std::string release_key;
    std::string messages;
  };

  using namespace_data_ptr = std::unique_ptr<namespace_data>;

  struct callback_entry {
    std::string _namespace_name;
    std::string _key;
    update_callback _callback;
  };

  mutable std::shared_mutex _namespaces_mutex;
  std::unordered_map<std::string, namespace_data_ptr> _namespaces;

  mutable std::shared_mutex _callbacks_mutex;
  std::unordered_map<std::string, std::unordered_map<std::string, std::vector<update_callback>>> _callbacks;

  std::atomic_bool _running{false};
  std::thread _poll_thread;
 public:
  apollo_client() = default;

  ~apollo_client() {
    stop();
  }

  static std::unique_ptr<apollo_client> create(
    std::string_view address, std::string_view app_id, std::string_view cluster, std::string_view request_ip = {}) {
    auto client = std::make_unique<apollo_client>();
    client->_address = address;
    client->_app_id = app_id;
    client->_cluster = cluster;
    client->_request_ip = request_ip;
    return client;
  }

  bool add_namespace(std::string_view namespace_name) {
    {
      std::unique_lock lock(_namespaces_mutex);
      auto [it, inserted] = _namespaces.try_emplace(std::string(namespace_name), std::make_unique<namespace_data>());
      if (!inserted)
        return false;
    }
    return fetch_config(namespace_name);
  }

  void register_callback(std::string_view namespace_name, std::string_view key, update_callback callback) {
    std::unique_lock lock(_callbacks_mutex);
    _callbacks[std::string(namespace_name)][std::string(key)].push_back(std::move(callback));
  }

  void start() {
    _running.store(true);
    _poll_thread = std::thread([this] { long_poll_loop(); });
  }
  void stop() noexcept {
    _running.store(false);
    if (_poll_thread.joinable()) {
      _poll_thread.join();
    }
  }

  std::optional<std::string> get_value(std::string_view namespace_name, std::string_view key) const noexcept;

private:
  void long_poll_loop() {
   while (_running) {
      perform_long_poll();
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  bool fetch_config(std::string_view namespace_name) {
    util::circuit_breaker::raii_scoped_attempt attempt(_breaker);
    if (!attempt) {
      return false;
    }

    try {
      auto request_path = std::format("/configs/{}/{}/{}", _app_id, _cluster, namespace_name);
      auto params = build_configs_params(namespace_name);
      if (!params.empty()) {
        request_path += "?" + params;
        if (!_request_ip.empty()) {
          request_path += std::format("&ip={}", _request_ip);
        }
      } else if (!_request_ip.empty()) {
        request_path += std::format("?ip={}", _request_ip);
      }

      auto& cli = get_client();
      std::string response_body;
      if (auto res = cli.Get(
            request_path,
            [&response_body](const httplib::Response& response) {
              // cancel if response status code = 304, empty body
              if (response.status == 304)
                return false;
              if (response.content_length_ > response_body.max_size()) {
                return false;
              }
              response_body.reserve(response.content_length_);
              return true;
            },
            [&response_body](const char* data, size_t length) {
              response_body.append(data, length);
              return true;
            })) {
        if (res->status == 200) {
          process_config_response(namespace_name, response_body);

          attempt.set_success();
          return true;
        }
      }
    } catch (...) {
      // TODO: log error
    }
    return false;
  }

  void process_config_response(std::string_view namespace_name, const std::string& response) {
    try {
      {
        auto root = json::parse(response);
        auto ns_data = std::make_unique<namespace_data>();

        std::unique_lock data_lock(ns_data->mutex);
        ns_data->cache_configs.clear();
        for (auto& [key, value] : root["configurations"].items()) {
          ns_data->cache_configs[key] = value.get<std::string>();
        }
        ns_data->release_key = root["releaseKey"].get<std::string>();

        std::unique_lock lock(_namespaces_mutex);
        _namespaces[std::string(namespace_name)] = std::move(ns_data);
      }

      trigger_callbacks(namespace_name);
    } catch (const json::exception& ex) {
      // TODO: log error
    }
  }

  void trigger_callbacks(std::string_view namespace_name) {
    std::shared_lock ns_lock(_namespaces_mutex);
    if (auto ns_it = _namespaces.find(std::string(namespace_name)); ns_it != _namespaces.end()) {
      std::unordered_map<std::string, std::string> current_config;
      {
        std::shared_lock data_lock(ns_it->second->mutex);
        current_config = ns_it->second->cache_configs;  // copy out
      }

      std::shared_lock cb_lock(_callbacks_mutex);
      if (auto cb_ns_it = _callbacks.find(std::string(namespace_name)); cb_ns_it != _callbacks.end()) {
        for (const auto& [key, value] : current_config) {
          if (auto cb_key_it = cb_ns_it->second.find(key); cb_key_it != cb_ns_it->second.end()) {
            for (const auto& cb : cb_key_it->second) {
              // thread_pool::instance().submit([=] { cb(key, value); });
              cb(key, value);
            }
          }
        }
      }
    }
  }

  void perform_long_poll() {
    std::string notifications = build_notifications_list();
    notifications = strict_url_encode(notifications);

    auto request_path =
      std::format("/notifications/v2?appId={}&cluster={}&notifications={}", _app_id, _cluster, notifications);
    auto& cli = get_client(true);
    std::string response_body;
    if (auto res = cli.Get(
          request_path,
          [&response_body](const httplib::Response& response) {
            // cancel if response status code = 304, empty body
            if (response.status == 304)
              return false;
            if (response.content_length_ > response_body.max_size()) {
              return false;
            }
            response_body.reserve(response.content_length_);
            return true;
          },
          [&response_body](const char* data, size_t length) {
            response_body.append(data, length);
            return true;
          })) {
      if (res->status == 200) {
        process_notifications(response_body);
      }
    }
  }

  std::string build_configs_params(std::string_view namespace_name) const {
    std::string params;
    std::shared_lock ns_lock(_namespaces_mutex);
    if (auto ns_it = _namespaces.find(std::string(namespace_name)); ns_it != _namespaces.end()) {
      std::shared_lock data_lock(ns_it->second->mutex);
      if (!ns_it->second->release_key.empty()) {
        params = std::format("releaseKey={}", ns_it->second->release_key);
      }
      if (!ns_it->second->messages.empty()) {
        if (!params.empty()) {
          params += "&";
        }
        params += std::format("messages={}", strict_url_encode(ns_it->second->messages));
      }
    }
    return params;
  }

  std::string build_notifications_list() const {
    std::shared_lock lock(_namespaces_mutex);
    std::string notifications = "[";
    for (const auto& [ns_name, ns_data] : _namespaces) {
      std::shared_lock data_lock(ns_data->mutex);
      notifications +=
        std::format(R"({{"namespaceName":"{}","notificationId":{}}},)", ns_name, ns_data->notification_id);
    }
    if (!notifications.empty()) {
      notifications.pop_back();
    }
    notifications += "]";
    return notifications;
  }

  void process_notifications(const std::string& response) {
    try {
      auto notifications = json::parse(response);
      for (const auto& item : notifications) {
        auto ns_name = item["namespaceName"].get<std::string>();
        auto new_id = item["notificationId"].get<int64_t>();
        auto new_messages = item["messages"].get<json>();
        update_notification_data(ns_name, new_id, new_messages.dump());
        fetch_config(ns_name);
      }
    } catch (const json::exception&) {
    }
  }

  void update_notification_data(const std::string& ns_name, int64_t new_id, const std::string& new_messages) {
    std::shared_lock lock(_namespaces_mutex);
    if (auto it = _namespaces.find(ns_name); it != _namespaces.end()) {
      std::unique_lock data_lock(it->second->mutex);
      it->second->notification_id = new_id;
      it->second->messages = new_messages;
    }
  }

  static std::string strict_url_encode(const std::string& value) {
    static const char hex[] = "0123456789ABCDEF";
    std::string escaped;
    escaped.reserve(value.size() * 3);

    for (unsigned char c : value) {
      if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
        escaped += c;
      } else {
        escaped += '%';
        escaped += hex[(c >> 4) & 0xF];
        escaped += hex[c & 0xF];
      }
    }
    return escaped;
  }

  httplib::Client& get_client(bool long_poll = false) const {
    static thread_local httplib::Client cli(_address);
    cli.set_keep_alive(true);
    cli.set_read_timeout(long_poll ? 70 : 10);
    cli.set_write_timeout(10);
    cli.set_connection_timeout(5);
    cli.set_compress(true);
    cli.set_decompress(true);
    cli.set_follow_location(false);
    return cli;
  }
};
}  // namespace client
}  // namespace apollo
