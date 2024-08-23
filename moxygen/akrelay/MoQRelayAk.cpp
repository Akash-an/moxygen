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
#include <folly/experimental/coro/BlockingWait.h>

namespace moxygen {

folly::coro::Task<void> MoQRelayAk::onAnnounce(Announce ann, std::shared_ptr<MoQSession> session) {
  // check auth
  XLOG(DBG1) << "onAnnounce IN RELAY_AK" << ann.trackNamespace;
  if (ann.trackNamespace.starts_with(allowedNamespacePrefix_)) {
    session->announceOk({ann.trackNamespace});
    // insert into db; ak-todo: error handling: what if insert fails
    auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
    co_await harperdb.executeInsertQuery(ann.trackNamespace, true);
    announces_.emplace(std::move(ann.trackNamespace), std::move(session));
    XLOG(INFO) << "announced " << ann.trackNamespace; 
    co_return;
  } else {
    session->announceError({ann.trackNamespace, 403, "bad namespace"});
  }
}

folly::coro::Task<void> MoQRelayAk::onSubscribe(
    SubscribeRequest subReq,
    std::shared_ptr<MoQSession> session) {

  XLOG(INFO) << "onSubscribe IN RELAY_AK";
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

    //ak-todo: remove this
    XLOG(INFO) << "ANNOUNCES_ size: " << announces_.size();
    for(auto& it : announces_) {
      XLOG(INFO) << it.first;
    }

    if (upstreamSessionIt == announces_.end()) {
      // no such namespace has been announced
      // check if the namespace exists in the database.

      XLOG(INFO) << "checking db";

      auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
      auto relay_hostname = co_await harperdb.getNearestRelay(subReq.fullTrackName.trackNamespace);

      
      if (relay_hostname=="") {
        session->subscribeError({subReq.subscribeID, 404, "namespace not found"});
        co_return;
      }
      
      auto relay_url = "https://" + relay_hostname + ":4433/moq";

      folly::StringPiece url_fw(relay_url);

      XLOG(DBG1) << "constructed relay url: " << url_fw;
      auto relay_client_ = std::make_unique<MoQRelayClientAk> (
          session->getEventBase(),
          proxygen::URL{url_fw}
      );

      relay_client_->run(Role::SUBSCRIBER, {subReq}).scheduleOn(session->getEventBase()).start();
      XLOG(INFO) << "started a relay client session to peer";      
      auto sub_session_ftr = co_await co_awaitTry(std::move(relay_client_->sessionContract_.second));

      XLOG(INFO) << "successfully scheduled";
      // auto sub_session = relay_client_->getMoQSession();

      relay_clients_.push_back(std::move(relay_client_));
      if (sub_session_ftr.hasException()) {
        XLOG(INFO) << "failed to create session";
        co_return;
      }

      auto sub_session = std::move(sub_session_ftr.value());
        
      // auto trackNamespaceCopy = subReq.fullTrackName.trackNamespace;

      announces_.emplace(subReq.fullTrackName.trackNamespace, std::move(sub_session));
      XLOG(INFO) << "Emplacing namespace: " << subReq.fullTrackName.trackNamespace;

      //todo: you can just construct the iterator.. just find out how to.
      upstreamSessionIt = announces_.find(subReq.fullTrackName.trackNamespace);      
      if (upstreamSessionIt == announces_.end()){
        XLOG(INFO) << "ITS NULL ";
      }

      //add to tracker database
      // auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
      co_await harperdb.executeInsertQuery(subReq.fullTrackName.trackNamespace, false);
      XLOG(INFO) << "added namespace to database for relay";
    }
    
    if (session.get() == upstreamSessionIt->second.get()) {
      session->subscribeError({subReq.subscribeID, 400, "self subscribe"});
      XLOG(INFO) << "used co_return; self subscribe";
      co_return;
    }
    XLOG(INFO) <<"namespace exist locally in the announces";


    //session subscribe
    auto subRes = co_await upstreamSessionIt->second->subscribe(subReq);
    if (subRes.hasError()) {
      session->subscribeError({subReq.subscribeID, 502, "subscribe failed"});
      XLOG(INFO) << "used co_return; subscription failed in upstream";
      co_return;
    }

    forwarder = std::make_shared<MoQForwarderAk>(
        subReq.fullTrackName, subRes.value()->latest());
    RelaySubscription rsub(
        {forwarder,
         upstreamSessionIt->second,
         (*subRes)->subscribeID(),
         std::move(folly::CancellationSource())});
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
  XLOG(INFO) << "onUnsubscribe Relay: "<< unsub.subscribeID;
  for (auto subscriptionIt = subscriptions_.begin();subscriptionIt != subscriptions_.end();) {
    if (subscriptions_.empty()) {
      XLOG(INFO) << "no subscriptions left.. we shouldn't be here";
          // break;
    }
    auto& subscription = subscriptionIt->second;
    subscription.forwarder->removeSession(session, unsub.subscribeID);
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for " << subscriptionIt->first.trackNamespace << "/"<< subscriptionIt->first.trackName;
      subscription.cancellationSource.requestCancellation();
      auto tracknamespace = subscriptionIt->first.trackNamespace;
      
      //forward the unsubscribe to the upstream publisher
      subscription.upstream->unsubscribe(Unsubscribe{subscription.subscribeID});
      
      XLOG(INFO) << "Removing from announces, now len is: " << announces_.size();
      auto ann_it = announces_.find(tracknamespace);
      if (ann_it != announces_.end()) {
        announces_.erase(ann_it);
      }
      XLOG(INFO) << "Removed from announces, now len is: " << announces_.size();
      // if (!announces_.empty()) {
      //   for (auto it = announces_.begin(); it != announces_.end();) {
      //     if (it->first == tracknamespace) {
      //       it = announces_.erase(it);
      //     } else {
      //       it++;
      //     }
      //   }
      // }
      
      XLOG(INFO) << "removing from subscriptions, now len is: " << subscriptions_.size();
      subscriptionIt = subscriptions_.erase(subscriptionIt);
      XLOG(INFO) << "removed from subscriptions, now len is: " << subscriptions_.size();

      XLOG(INFO) << "Removing from database";
      //remove entry from tracker if it was not the originalpublisher
      auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
      co_await harperdb.executeDeleteQuery(tracknamespace, false);
      XLOG(INFO) << "removed from database";
    
    } else {
      subscriptionIt++;
    }
  }
}


// void MoQRelayAk::onUnsubscribe(
//     Unsubscribe unsub,
//     std::shared_ptr<MoQSession> session) {
//   // TODO: session+subscribe ID should uniquely identify this subscription,
//   // we shouldn't need a linear search to find where to remove it.
//   for (auto subscriptionIt = subscriptions_.begin();
//        subscriptionIt != subscriptions_.end();) {
//     auto& subscription = subscriptionIt->second;
//     subscription.forwarder->removeSession(session, unsub.subscribeID);
//     if (subscription.forwarder->empty()) {
//       XLOG(INFO) << "Removed last subscriber for "
//                  << subscriptionIt->first.trackNamespace
//                  << subscriptionIt->first.trackName;
//       // subscription.cancellationSource.requestCancellation();
//       subscription.upstream->unsubscribe({subscription.subscribeID});
//       subscriptionIt = subscriptions_.erase(subscriptionIt);
//     } else {
//       subscriptionIt++;
//     }
//   }
// }

void MoQRelayAk::removeSession(const std::shared_ptr<MoQSession>& session) {
  // TODO: remove linear search
  if (!session) {
    //necessary as it is called on transport error as well;
    XLOG (INFO) << "Relay removeSession: no session.. why are we here?";
    return;
  }

  XLOG(INFO) << " Relay removeSession: ";// << session->id;
  bool remove_from_db = false;
  std::string tracknamespace; 
  XLOG(INFO) << "Removing from announces, now len is: " << announces_.size();
  for (auto it = announces_.begin(); it != announces_.end();) {
    if (it->second.get() == session.get()) {
      remove_from_db = true;
      tracknamespace = it->first;
      it = announces_.erase(it);
    } else {
      it++;
    }
  }
  XLOG(INFO) << "Removed from announces, now len is: " << announces_.size();

  // TODO: we should keep a map from this session to all its subscriptions
  // and remove this linear search also

  XLOG(INFO) << "Relay removeSession: subscriptions size: " << subscriptions_.size();
  // if (subscriptions_.empty()) {
  //   return;
  // }

  for (auto subscriptionIt = subscriptions_.begin(); subscriptionIt != subscriptions_.end();) {

    if (subscriptions_.empty()) {
      XLOG(INFO) << "no subscriptions left.. we shouldn't be here";
      // break;
    }
    bool remove_sub = false;   
    auto& subscription = subscriptionIt->second;
    tracknamespace = subscriptionIt->first.trackNamespace;
    XLOG(INFO) << "Relay removeSession: track: " << subscriptionIt->first.trackNamespace + " " + subscriptionIt->first.trackName;
    XLOG(INFO) << "Relay removeSession: subscription id: " << subscription.subscribeID;
    if (subscription.upstream.get() == session.get()) {
      // its an upstream disconnect
      subscription.forwarder->error(
          SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream disconnect");
      subscription.cancellationSource.requestCancellation();
      remove_from_db = true;
      remove_sub = true;
    } else {
      // its a downstream disconnect
      subscription.forwarder->removeSession(session); 
      if (subscription.forwarder->empty()) {
        XLOG(INFO) << "Removed last subscriber for "<< subscriptionIt->first.trackNamespace << subscriptionIt->first.trackName;
        subscription.upstream->unsubscribe({subscription.subscribeID});
        remove_sub = true;
        remove_from_db = true;
        subscription.cancellationSource.requestCancellation();
        
      } 
    }

    if (remove_sub) {
      XLOG(INFO) << "removing from subscriptions, now len is: " << subscriptions_.size();
      subscriptionIt = subscriptions_.erase(subscriptionIt);
      XLOG(INFO) << "removed from subscriptions, now len is: " << subscriptions_.size();
    } else {
      subscriptionIt++;
    }
    
  }

  //remove from db if needed
  if (remove_from_db) {
    // auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
    // harperdb_->executeDeleteQuery(std::move(tracknamespace), true).scheduleOn(std::move(session->getEventBase())).start();

    harperdb_ = std::make_unique<moxygen::HarperDBQuery>(folly::EventBaseManager::get()->getEventBase());
    harperdb_->executeDeleteQuery(tracknamespace, true).scheduleOn(folly::EventBaseManager::get()->getEventBase()).start();
    XLOG(INFO) << "Removed from database";
  }

}

folly::coro::Task<void> MoQRelayAk::onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session){
  
  XLOG(INFO) << "RelayAK onUnannounce: " << unAnn.trackNamespace;
  //remove tracknamespace from your announces
  // for (auto it = announces_.begin(); it != announces_.end();) {
  //   if (it->first == unAnn.trackNamespace) {
  //     it = announces_.erase(it);
  //   } else {
  //     it++;
  //   }
  // }

  auto it = announces_.find(unAnn.trackNamespace);
  if (it != announces_.end()) {
    announces_.erase(it);
  }

  // if (subscriptions_.empty()) {
  //   co_return;
  // }

  //remove corresponding subscription and send unannounce to clients
  for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
    if (subscriptions_.empty()) {
      XLOG(ERR) << "subscriptions_.empty().. why enter the loop??";
      // break;
    }
    if (it->first.trackNamespace == unAnn.trackNamespace) {
      
      auto subscription = it->second;
      subscription.cancellationSource.requestCancellation();

      //send unannounce to subscribers
      subscription.forwarder->forwardUnannounce(unAnn);
      
      XLOG(INFO) << "removing from subscriptions, now len is: " << subscriptions_.size();
      it = subscriptions_.erase(it);
      XLOG(INFO) << "removed from subscriptions, now len is: " << subscriptions_.size();
    } else {
      it++;
    }
  }

  XLOG(DBG1) << "Removing from database";
  auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
  // folly::BlockingWait(harperdb.executeDeleteQuery(unAnn.trackNamespace, true));
  // harperdb_ = std::make_unique<moxygen::HarperDBQuery>(folly::EventBaseManager::get()->getEventBase());
  co_await harperdb.executeDeleteQuery(unAnn.trackNamespace, true);
  XLOG(DBG1) << "removed from database";
  co_return;
}


folly::coro::Task<void> MoQRelayAk::onSubscribeDone(SubscribeDone subscribeDone, std::shared_ptr<MoQSession> session){

// if (subscriptions_.empty()) {
//     co_return;
//   }
  //remove corresponding subscription and send subscribe_done to clients
  for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
    if (subscriptions_.empty()) {
      XLOG(ERR) << "subscriptions_.empty().. why enter the loop??";
      // break;
    }
    if (it->second.subscribeID == subscribeDone.subscribeID) {
      auto subscription = it->second;
      
      //this sends subscribe_done to subscribers
      //todo change this forward unannounces
      subscription.forwarder->error(
        SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream subscribe done"
      );

      subscription.cancellationSource.requestCancellation();

      XLOG(INFO) << "removing from subscriptions, now len is: " << subscriptions_.size();
      it = subscriptions_.erase(it);
      XLOG(INFO) << "removed from subscriptions, now len is: " << subscriptions_.size();

    } else {
      it++;
    }
  }
  co_return;
}


} // namespace moxygen
