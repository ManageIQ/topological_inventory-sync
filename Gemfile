source "https://rubygems.org"

plugin "bundler-inject", "~> 1.1"
require File.join(Bundler::Plugin.index.load_paths("bundler-inject")[0], "bundler-inject") rescue nil

gem "cloudwatchlogger", "~> 0.2"
gem "http",             "~> 4.1.0", :require => false
gem "json-stream",      "~> 0.2.0", :require => false
gem "manageiq-loggers", "~> 0.4.0", ">= 0.4.2"
gem "manageiq-messaging"
gem "more_core_extensions"
gem "optimist"
gem "prometheus_exporter", "~> 0.4.5"
gem "rest-client",      ">= 1.8.0"

gem "sources-api-client", "~> 1.0"
gem "topological_inventory-ingress_api-client", "~> 1.0", ">= 1.0.1"
gem "topological_inventory-core", "~> 1.1.1"
gem "topological_inventory-api-client", "~> 2.0"
gem "topological_inventory-providers-common", "~> 0.1"

group :development, :test do
  gem "rake", ">= 12.3.3"
  gem "rspec-rails"
  gem 'rubocop',             "~>0.69.0", :require => false
  gem 'rubocop-performance', "~>1.3",    :require => false
  gem "simplecov",           "~>0.17.1"
  gem "timecop"
  gem "webmock"
end
