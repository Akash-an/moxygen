/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/akrelay/MoQRelayAk.h"
#include "MoQRelayClientAk.h"
#include "../MoQServer.h"


#include <proxygen/lib/http/HTTPConnector.h>

#include <chrono>

#include <folly/experimental/coro/Sleep.h>

namespace moxygen {

folly::coro::Task<void> MoQRelayAk::onAnnounce(Announce ann, std::shared_ptr<MoQSession> session) {
  // check auth
  XLOG(INFO) << "onAnnounce IN RELAYak" << ann.trackNamespace;
  if (ann.trackNamespace.starts_with(allowedNamespacePrefix_)) {
    session->announceOk({ann.trackNamespace});
    // insert into db
    auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
    // co_await harperdb.executeInsertQuery(ann.trackNamespace, true);
    announces_.emplace(std::move(ann.trackNamespace), std::move(session));
    XLOG(INFO) << "announced " << ann.trackNamespace;
 
    // co_await  folly::coro::sleep(std::chrono::seconds(1));
    co_return;
  } else {
    session->announceError({ann.trackNamespace, 403, "bad namespace"});
  }
}

folly::coro::Task<void> MoQRelayAk::onSubscribe(
    SubscribeRequest subReq,
    std::shared_ptr<MoQSession> session) {
  auto subscriptionIt = subscriptions_.find(subReq.fullTrackName);
  std::shared_ptr<MoQForwarderAk> forwarder;
  if (subscriptionIt == subscriptions_.end()) {
    // first subscriber

    // check auth
    // get trackNamespace
    if (subReq.fullTrackName.trackNamespace.empty()) {
      session->subscribeError({subReq.subscribeID, 400, "namespace required"});
      co_return;
    }
    auto upstreamSessionIt = announces_.find(subReq.fullTrackName.trackNamespace);

    if (upstreamSessionIt == announces_.end()) {
      // no such namespace has been announced
      // check if the namespace exists in the peer.

      XLOG(INFO) << "checking db";

      auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
      auto relay_hostname = co_await harperdb.getNearestRelay(subReq.fullTrackName.trackNamespace);

      XLOG(INFO) << "relay_hostname=" << relay_hostname;
      if (relay_hostname=="") {
        session->subscribeError({subReq.subscribeID, 404, "namespace not found"});
        co_return;
      }


      folly::StringPiece url_fw(relay_hostname);
      // auto controllerFn = [](std::shared_ptr<MoQSession> session) {
      //   // auto control = MoQServer::makeControlVisitor(clientSession);
      //     return std::make_unique<MoQSession::ControlVisitor>();
      // };
      XLOG(INFO) << "we are here";
      auto relay_client_ = std::make_unique<MoQRelayClientAk> (
          session->getEventBase(),
          proxygen::URL{url_fw}
      );

      relay_client_->run(Role::SUBSCRIBER, {subReq}).scheduleOn(session->getEventBase()).start();
            XLOG(INFO) << "we are here 2";      
      auto sub_session_ftr = co_await co_awaitTry(std::move(relay_client_->sessionContract_.second));

      XLOG(INFO) << "successfully scheduled";
      // auto sub_session = relay_client_->getMoQSession();

      relay_clients_.push_back(std::move(relay_client_));
      if (sub_session_ftr.hasException()) {
        XLOG(INFO) << "failed to create session";
        co_return;
      }

      auto sub_session = std::move(sub_session_ftr).value();
        
      auto trackNamespaceCopy = subReq.fullTrackName.trackNamespace;

      announces_.emplace(std::move(trackNamespaceCopy), std::move(sub_session));
      XLOG(INFO) << "Emplacing namespace: " << subReq.fullTrackName.trackNamespace;



      upstreamSessionIt = announces_.find(subReq.fullTrackName.trackNamespace);      
      XLOG(INFO) << "got here 1";

      if (upstreamSessionIt == announces_.end()){
        XLOG(INFO) << "ITS NULL 1";
      }


      //add to tracker database
      // auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
      // co_await harperdb.executeInsertQuery(subReq.fullTrackName.trackNamespace, false);


      // session->subscribeError({subReq.subscribeID, 404, "no such namespace"});
      // co_return;
    }
    
    if (session.get() == upstreamSessionIt->second.get()) {
      session->subscribeError({subReq.subscribeID, 400, "self subscribe"});
      XLOG(INFO) << "used co_return 1";
      co_return;
    }
          XLOG(INFO) << "got here 2";



    auto subRes = co_await upstreamSessionIt->second->subscribe(subReq);
    if (subRes.hasError()) {
      session->subscribeError({subReq.subscribeID, 502, "subscribe failed"});
            XLOG(INFO) << "used co_return 2";

      co_return;
    }

    forwarder = std::make_shared<MoQForwarderAk>(
        subReq.fullTrackName, subRes.value()->latest());
    RelaySubscription rsub(
        {forwarder,
         upstreamSessionIt->second,
         (*subRes)->subscribeID(),
         folly::CancellationSource()});
    auto token = rsub.cancellationSource.getToken();
    subscriptions_[subReq.fullTrackName] = std::move(rsub);
    folly::coro::co_withCancellation(
        token, forwardTrack(subRes.value(), forwarder))
        .scheduleOn(upstreamSessionIt->second->getEventBase())
        .start();


  } else {
    forwarder = subscriptionIt->second.forwarder;
  }
  // Add to subscribers list
  forwarder->addSubscriber(
      session, subReq.subscribeID, subReq.trackAlias, subReq);
  session->subscribeOk(
      {subReq.subscribeID, std::chrono::milliseconds(0), forwarder->latest()});
}

folly::coro::Task<void> MoQRelayAk::forwardTrack(
    std::shared_ptr<MoQSession::TrackHandle> track,
    std::shared_ptr<MoQForwarderAk> fowarder) {
       XLOG(DBG1) << __func__ << " start";
  while (auto obj = co_await track->objects().next()) {
    XLOG(DBG1) << __func__
               << " new object t=" << obj.value()->fullTrackName.trackNamespace
               << obj.value()->fullTrackName.trackName
               << " g=" << obj.value()->header.group
               << " o=" << obj.value()->header.id;
    folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
    uint64_t payloadOffset = 0;
    bool eom = false;
    while (!eom) {
      auto payload = co_await obj.value()->payloadQueue.dequeue();
      if (payload) {
        payloadBuf.append(std::move(payload));
        XLOG(DBG1) << __func__
                   << " object bytes, buflen now=" << payloadBuf.chainLength();
      } else {
        XLOG(DBG1) << __func__
                   << " object eom, buflen now=" << payloadBuf.chainLength();
        eom = true;
      }
      auto payloadLength = payloadBuf.chainLength();
      if (eom || payloadOffset + payloadLength > 1280) {
        fowarder->publish(
            obj.value()->header, payloadBuf.move(), payloadOffset, eom);
        payloadOffset += payloadLength;
      } else {
        XLOG(DBG1) << __func__
                   << " Not publishing yet payloadOffset=" << payloadOffset
                   << " payloadLength=" << payloadLength
                   << " eom=" << uint64_t(eom);
      }
    }
  }
}

folly::coro::Task<void> MoQRelayAk::onUnsubscribe(
    Unsubscribe unsub,
    std::shared_ptr<MoQSession> session) {
  // TODO: session+subscribe ID should uniquely identify this subscription,
  // we shouldn't need a linear search to find where to remove it.
  for (auto subscriptionIt = subscriptions_.begin();subscriptionIt != subscriptions_.end();) {
    auto& subscription = subscriptionIt->second;
    subscription.forwarder->removeSession(session, unsub.subscribeID);
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for "
                 << subscriptionIt->first.trackNamespace
                 << subscriptionIt->first.trackName;
      subscription.cancellationSource.requestCancellation();
      subscription.upstream->unsubscribe({subscription.subscribeID});


      XLOG(INFO) << "Removing from database";
      //remove entry from tracker if it was not the originalpublisher
      auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
      co_await harperdb.executeDeleteQuery(subscriptionIt->first.trackNamespace, false);
      XLOG(INFO) << "removed from database";

      subscriptionIt = subscriptions_.erase(subscriptionIt);

    } else {
      subscriptionIt++;
    }
  }
}

void MoQRelayAk::removeSession(const std::shared_ptr<MoQSession>& session) {
  // TODO: remove linear search
  for (auto it = announces_.begin(); it != announces_.end();) {
    if (it->second.get() == session.get()) {
      it = announces_.erase(it);
    } else {
      it++;
    }
  }
  // TODO: we should keep a map from this session to all its subscriptions
  // and remove this linear search also
  for (auto subscriptionIt = subscriptions_.begin(); subscriptionIt != subscriptions_.end();) {
    auto& subscription = subscriptionIt->second;
    //akash todo: it can be neither upstream nor downstream.. 
    if (subscription.upstream.get() == session.get()) {
      subscription.forwarder->error(
          SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream disconnect");
      subscription.cancellationSource.requestCancellation();
    } else {
      subscription.forwarder->removeSession(session);
    }
    
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for "
                 << subscriptionIt->first.trackNamespace
                 << subscriptionIt->first.trackName;
      subscription.upstream->unsubscribe({subscription.subscribeID});
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}


folly::coro::Task<void> MoQRelayAk::onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session){
  
  //removed tracknamespace from your announces
  for (auto it = announces_.begin(); it != announces_.end();) {
    if (it->first == unAnn.trackNamespace) {
      it = announces_.erase(it);
    } else {
      it++;
    }
  }

  //remove corresponding subscription and send subscribe_done to clients
  for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
    if (it->first.trackNamespace == unAnn.trackNamespace) {
      auto subscription = it->second;
      auto upstream_session = subscription.upstream;
      
      //this sends subscribe_done to subscribers
      //todo change this forward unannounces
      subscription.forwarder->error(
        SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream unannounce"
      );

      // subscription.cancellationSource.requestCancellation();

      it = subscriptions_.erase(it);
    } else {
      it++;
    }
  }

  XLOG(INFO) << "Removing from database";
  //remove entry from tracker
  auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
  co_await harperdb.executeDeleteQuery(unAnn.trackNamespace, true);
  XLOG(INFO) << "removed from database";
  co_return;
}


folly::coro::Task<void> MoQRelayAk::onSubscribeDone(SubscribeDone subscribeDone, std::shared_ptr<MoQSession> session){

  //remove corresponding subscription and send subscribe_done to clients
  for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
    if (it->second.subscribeID == subscribeDone.subscribeID) {
      auto subscription = it->second;
      
      //this sends subscribe_done to subscribers
      //todo change this forward unannounces
      subscription.forwarder->error(
        SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream subscribe done"
      );

      // subscription.cancellationSource.requestCancellation();

      it = subscriptions_.erase(it);
    } else {
      it++;
    }
  }
  co_return;
}


} // namespace moxygen
