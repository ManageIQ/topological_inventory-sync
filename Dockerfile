FROM registry.access.redhat.com/ubi8/ubi:8.3-227

RUN dnf -y --disableplugin=subscription-manager module enable ruby:2.6 && \
    dnf -y --disableplugin=subscription-manager --setopt=tsflags=nodocs install \
      ruby-devel \
      # To compile native gem extensions
      gcc-c++ \
      # For git based gems
      git make redhat-rpm-config \
      # For checking service status
      nmap-ncat \
      # Libraries
      postgresql-devel libxml2-devel \
      && \
    dnf --disableplugin=subscription-manager clean all

ENV WORKDIR /opt/topological_inventory-sync/
ENV RAILS_ROOT $WORKDIR
WORKDIR $WORKDIR

COPY Gemfile $WORKDIR
RUN echo "gem: --no-document" > ~/.gemrc && \
    gem install bundler --conservative --without development:test && \
    bundle install --jobs 8 --retry 3 && \
    find $(gem env gemdir)/gems/ | grep "\.s\?o$" | xargs rm -rvf && \
    rm -rvf $(gem env gemdir)/cache/* && \
    rm -rvf /root/.bundle/cache

COPY . $WORKDIR
COPY docker-assets/entrypoint /usr/bin
COPY docker-assets/run_sources_sync /usr/bin

RUN chgrp -R 0 $WORKDIR && \
    chmod -R g=u $WORKDIR

ENTRYPOINT ["entrypoint"]
CMD ["run_sources_sync"]
