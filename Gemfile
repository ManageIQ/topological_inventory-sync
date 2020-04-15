source "https://rubygems.org"

plugin "bundler-inject", "~> 1.1"
require File.join(Bundler::Plugin.index.load_paths("bundler-inject")[0], "bundler-inject") rescue nil

gem "cloudwatchlogger", "~> 0.2"
gem "http",             "~> 4.1.0", :require => false
gem "json-stream",      "~> 0.2.0", :require => false
gem "manageiq-loggers", "~> 0.4.0", ">= 0.4.2"
gem "manageiq-messaging"
gem "optimist"

gem "sources-api-client", "~> 1.0"
gem "topological_inventory-ingress_api-client", "~> 1.0"
gem "topological_inventory-core", "~> 1.0.0"
gem "topological_inventory-api-client", "~> 2.0"
gem "topological_inventory-providers-common", "~> 0.1"

group :development, :test do
  gem "rake"
  gem "rspec-rails"
  gem "simplecov"
  gem "webmock"
end
