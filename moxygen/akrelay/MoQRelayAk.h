/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQSession.h"
#include "moxygen/akrelay/MoQForwarderAk.h"
#include "moxygen/akrelay/MoQRelayClientAk.h"
#include "moxygen/akrelay/dbclient.h"
#include "moxygen/akrelay/dbquery.h"

#include <proxygen/lib/http/HTTPConnector.h>

#include <folly/container/F14Set.h>
#include <folly/io/async/EventBaseManager.h>

#include <list>


namespace moxygen {

class MoQRelayAk {
 public:

  MoQRelayAk(){
    harperdb_ = std::make_unique<moxygen::HarperDBQuery>(folly::EventBaseManager::get()->getEventBase());
  }
  void setAllowedNamespacePrefix(std::string allowed) {
    allowedNamespacePrefix_ = std::move(allowed);
  }

  folly::coro::Task<void> onAnnounce(Announce ann, std::shared_ptr<MoQSession> session);
  folly::coro::Task<void> onSubscribe(SubscribeRequest subReq, std::shared_ptr<MoQSession> session);
  // folly::coro::Task<void> onUnsubscribe(Unsubscribe unsub, std::shared_ptr<MoQSession> session);

void onUnsubscribe(Unsubscribe unsub, std::shared_ptr<MoQSession> session);



  folly::coro::Task<void> onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session);
  folly::coro::Task<void> onSubscribeDone(SubscribeDone subscribeDone, std::shared_ptr<MoQSession> session);

  void removeSession(const std::shared_ptr<MoQSession>& session);

  std::unique_ptr<moxygen::HarperDBQuery> harperdb_{nullptr};
 private:
  struct RelaySubscription {
    std::shared_ptr<MoQForwarderAk> forwarder;
    std::shared_ptr<MoQSession> upstream;
    uint64_t subscribeID;
    folly::CancellationSource cancellationSource;
  };
  folly::coro::Task<void> forwardTrack(
      std::shared_ptr<MoQSession::TrackHandle> track,
      std::shared_ptr<MoQForwarderAk> forwarder);

  std::string allowedNamespacePrefix_;
  folly::F14FastMap<std::string, std::shared_ptr<MoQSession>> announces_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash> subscriptions_;

  //to remove these?
  std::list<std::unique_ptr<moxygen::MoQRelayClientAk>> relay_clients_ = {};
  std::list<std::unique_ptr<proxygen::HTTPConnector>> db_clients_ = {};

};

} // namespace moxygen
