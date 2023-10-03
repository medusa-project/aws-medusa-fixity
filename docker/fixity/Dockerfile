FROM ruby:3.1.2

ENV RAILS_ENV=test
ENV RAILS_LOG_TO_STDOUT=true
ENV RUBY_HOME=/fixity

# Copy the Gemfile as well as the Gemfile.lock and install gems.
# This is a separate step so the dependencies will be cached.
RUN mkdir fixity
WORKDIR fixity

COPY Gemfile Gemfile.lock  ./
RUN gem install bundler && bundle install

# Copy the main application, except whatever is listed in .dockerignore.
COPY . ./

CMD ["rake", "test"]