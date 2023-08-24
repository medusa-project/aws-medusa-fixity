FROM ruby:3.1.2

RUN apt-get update && apt-get install -y \
  build-essential \
  git \
  cron

# Copy the Gemfile as well as the Gemfile.lock and install gems.
# This is a separate step so the dependencies will be cached.
COPY ../../Gemfile Gemfile.lock  ./
RUN gem install bundler && bundle install --without development test --jobs 20 --retry 5

COPY ../.. ./

## This is the web server entry point. It will need to be overridden when
## running the workers.
#CMD ["echo", "Error running task, please check the container override command!"]