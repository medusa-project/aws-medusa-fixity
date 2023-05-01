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

# Adding crontab to the appropriate location
ADD crontab /etc/cron.d/my-cron-file

# Giving permission to crontab file
RUN chmod 0644 /etc/cron.d/my-cron-file

# Running crontab
RUN crontab /etc/cron.d/my-cron-file

# Creating entry point for cron
ENTRYPOINT ["cron", "-f"]

## This is the web server entry point. It will need to be overridden when
## running the workers.
#CMD ["echo", "Error running task, please check the container override command!"]