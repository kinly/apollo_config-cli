#pragma once

#include <httplib.h>

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "cirtual_breaker.h"
#include "simdjson/singleheader/simdjson.h"

namespace apollo_config {

class client {
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
  simdjson::ondemand::parser _json_parser;

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
  client() = default;

  ~client() {
    stop();
  }

  static std::unique_ptr<client> create(
    std::string_view address, std::string_view app_id, std::string_view cluster, std::string_view request_ip = {}) {
    auto client_ = std::make_unique<client>();
    client_->_address = address;
    client_->_app_id = app_id;
    client_->_cluster = cluster;
    client_->_request_ip = request_ip;
    return client_;
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
    _poll_thread = std::thread([this] {
      long_poll_loop();
    });
  }
  void stop() noexcept {
    _running.store(false);
    if (_poll_thread.joinable()) {
      _poll_thread.join();
    }
  }

  std::optional<std::string> get_value(std::string_view namespace_name, std::string_view key) const noexcept {
    
  }

 private:
  void long_poll_loop() {
    while (_running) {
      perform_long_poll();
      std::this_thread::sleep_for(std::chrono::seconds(1));
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

      std::vector<char> response_body;
      response_body.reserve(8192);

      if (auto res = cli.Get(
            request_path,
            [&response_body](const httplib::Response& response) {
              // cancel if response status code = 304, empty body
              if (response.status == 304) {
                return false;
              }
              if (response.content_length_ > response_body.max_size()) {
                return false;
              }
              response_body.reserve(response.content_length_);
              return true;
            },
            [&response_body, namespace_name, this](const char* data, size_t length) {
              response_body.insert(response_body.end(), data, data + length);
              return true;
            })) {
        if (res->status == 200) {

          process_config_response(namespace_name, response_body);

          attempt.set_success();
          return true;
        }
      }
    } catch (const simdjson::simdjson_error& e) {
      std::cerr << "[fetch_config] JSON parsing error: " << e.what() << "\n";
    } catch (const std::exception& e) {
      std::cerr << "[fetch_config] Exception: " << e.what() << "\n";
    } catch (...) {
      std::cerr << "[fetch_config] Unknown exception\n";
    }
    return false;
  }

  void process_config_response(std::string_view namespace_name, std::vector<char>& response) {
    try {
      {
        // if using simdjson padded_string_view
        response.insert(response.end(), simdjson::SIMDJSON_PADDING, '\0');
        simdjson::padded_string_view psv(response.data(), response.size());
        auto doc = _json_parser.iterate(psv);

        auto ns_data = std::make_unique<namespace_data>();

        std::unique_lock data_lock(ns_data->mutex);
        ns_data->cache_configs.clear();
        for (auto field : doc["configurations"].get_object()) {
          ns_data->cache_configs.emplace(
            std::string_view(field.unescaped_key()), std::string_view(field.value().get_string().value()));
        }
        ns_data->release_key = std::string(doc["releaseKey"].get_string().value());

        std::unique_lock lock(_namespaces_mutex);
        _namespaces[std::string(namespace_name)] = std::move(ns_data);
      }

      trigger_callbacks(namespace_name);
    } catch (const simdjson::simdjson_error& e) {
      std::cerr << "[process_config_response] JSON parsing error: " << e.what() << "\n";
    } catch (const std::exception& e) {
      std::cerr << "[process_config_response] Exception: " << e.what() << "\n";
    } catch (...) {
      std::cerr << "[process_config_response] Unknown exception\n";
    }
  }

  bool fetch_configs(const std::vector<std::string>& namespaces) {
    std::vector<std::future<bool>> futures;
    constexpr size_t max_parallel = 8;
    std::atomic_bool any_success = false;

    for (const auto& ns : namespaces) {
      futures.emplace_back(std::async(std::launch::async, [this, ns, &any_success] {
        bool ok = fetch_config(ns);
        if (ok)
          any_success.store(true, std::memory_order_relaxed);
        return ok;
      }));

      if (futures.size() >= max_parallel) {
        for (auto& f : futures)
          f.wait();
        futures.clear();
      }
    }
    for (auto& f : futures)
      f.wait();
    return any_success.load();
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
        process_notifications_response(response_body);
      }
    }
  }

  void process_notifications_response(std::string& response) {
    try {
      {
        // if using simdjson padded_string_view
        response.resize(response.size() + simdjson::SIMDJSON_PADDING, '\0');
        simdjson::padded_string_view psv(response.data(), response.size());
        auto doc = _json_parser.iterate(psv);

        std::vector<std::string> namespaces;

        for (auto one : doc.get_array()) {
          auto ns_name = one["namespaceName"].get_string().value();
          auto new_id = one["notificationId"].get_int64().value();
          auto new_messages = one["messages"].get_object().value().raw_json().value();
          update_notification_data(ns_name, new_id, new_messages);

          namespaces.emplace_back(ns_name);
        }

        fetch_configs(namespaces);
      }
    } catch (const simdjson::simdjson_error& e) {
      std::cerr << "[process_notifications_response] JSON parsing error: " << e.what() << "\n";
    } catch (const std::exception& e) {
      std::cerr << "[process_notifications_response] Exception: " << e.what() << "\n";
    } catch (...) {
      std::cerr << "[process_notifications_response] Unknown exception\n";
    }
  }

  void update_notification_data(const std::string_view ns_name, int64_t new_id, const std::string_view new_messages) {
    std::shared_lock lock(_namespaces_mutex);
    if (auto it = _namespaces.find(std::string(ns_name)); it != _namespaces.end()) {
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
}  // namespace apollo_config
