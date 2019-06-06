source "https://rubygems.org"

plugin "bundler-inject", "~> 1.1"
require File.join(Bundler::Plugin.index.load_paths("bundler-inject")[0], "bundler-inject") rescue nil

gem "manageiq-loggers", "~> 0.1.0"
gem "manageiq-messaging"
gem "optimist"

gem "sources-api-client",                       :git => "https://github.com/ManageIQ/sources-api-client-ruby", :branch => "master"
gem "topological_inventory-core",               :git => "https://github.com/ManageIQ/topological_inventory-core", :branch => "master"
gem "topological_inventory-ingress_api-client", :git => "https://github.com/ManageIQ/topological_inventory-ingress_api-client-ruby", :branch => "master"

gem "typhoeus"
gem 'http'

group :development, :test do
  gem "rake"
  gem "rspec-rails"
  gem "simplecov"
  gem "webmock"
end
