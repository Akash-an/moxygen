/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include <folly/lang/Exception.h>

#include <proxygen/lib/utils/URL.h>

#include "moxygen/MoQClient.h"

#include "moxygen/akrelay/dbclient.h"
#include "moxygen/akrelay/dbquery.h"

namespace moxygen {

class MoQRelayClientAk {
 public:
  MoQRelayClientAk(
      folly::EventBase* evb,
      proxygen::URL url,
      std::shared_ptr< folly::F14FastMap<std::string, std::shared_ptr<MoQSession>> > announceMap)
      // std::shared_ptr<folly::F14FastMap<FullTrackName, moxygen::MoQRelayAk::RelaySubscription, FullTrackName::hash> > subscriptions) 
        : evb_(evb), url_(std::move(url)), announceMap_(announceMap)/*, subscriptions_(subscriptions)*/ {}
     

  ~MoQRelayClientAk() = default;

  struct MoQClientError{
    int code;
    std::string message;
  };
  
  class MoQSessionData {
    public:
    std::unordered_map<std::shared_ptr<MoQSession>, std::set<std::shared_ptr<MoQSession>> > downstreamSessions_;
    std::unordered_map<std::shared_ptr<MoQSession>, std::string> upstreamSessionTracknamespace_;

    
    //unordered map of client sessions and their clients
    std::unordered_map<std::shared_ptr<MoQSession>, std::shared_ptr<MoQClient> > clients_;
  };

  class MoQControlMessageHandler{

    public:
    MoQControlMessageHandler(std::shared_ptr<moxygen::MoQRelayClientAk::MoQSessionData> data) : data_(data){}
    
    folly::coro::Task<void> onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session);

    void addClient(std::shared_ptr<MoQClient> moqClient, std::shared_ptr<MoQSession> session);

    std::shared_ptr<moxygen::MoQRelayClientAk::MoQSessionData> data_;

  };

  class RelayClientControlVisitor : public MoQSession::ControlVisitor {
    public:
    explicit RelayClientControlVisitor(
        std::shared_ptr<MoQSession> clientSession,
        std::shared_ptr<moxygen::MoQRelayClientAk::MoQControlMessageHandler> controlMessageHandler,
        folly::EventBase* evb,
        MoQRelayClientAk& client) 
          : clientSession_(clientSession), controlMessageHandler_(controlMessageHandler), evb_(evb), client_(client) {}

    ~RelayClientControlVisitor() override = default;

    virtual void operator()(Announce announce) const  override{
      XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
    }

    virtual void operator()(SubscribeOk subscribeOk) const {
      XLOG(INFO) << "SubscribeOk id=" << subscribeOk.subscribeID;
      //resolve a promise here.
    }

    virtual void operator()(Unannounce unn) const override {
      XLOG(INFO) << "MOQ Client Unannounce ns=" << unn.trackNamespace;
      // controlMessageHandler_.onUnannounce(std::move(unn), clientSession_).scheduleOn(evb_).start();
      client_.onUnannounce(std::move(unn), clientSession_).scheduleOn(evb_).start();
    }

    virtual void operator()(Unsubscribe unsub) const override {
      XLOG(INFO) << "MoQ Client Unsubscribe id=" << unsub.subscribeID;
    }

    //copied
     virtual void operator()(ClientSetup /*setup*/) const {
      XLOG(INFO) << "ClientSetup";
    }
    virtual void operator()(ServerSetup setup) const {
      XLOG(INFO) << "ServerSetup, version=" << setup.selectedVersion;
    }

    virtual void operator()(AnnounceCancel announceCancel) const {
      XLOG(INFO) << "AnnounceCancel ns=" << announceCancel.trackNamespace;
    }

    virtual void operator()(AnnounceError announceError) const {
      XLOG(INFO) << "AnnounceError ns=" << announceError.trackNamespace
                 << " code=" << announceError.errorCode
                 << " reason=" << announceError.reasonPhrase;
    }

    virtual void operator()(SubscribeRequest subscribe) const {
      XLOG(INFO) << "Subscribe ftn=" << subscribe.fullTrackName.trackNamespace
                 << subscribe.fullTrackName.trackName;
    }

    virtual void operator()(SubscribeUpdateRequest subscribeUpdate) const {
      XLOG(INFO) << "SubscribeUpdate subID=" << subscribeUpdate.subscribeID;
    }

    virtual void operator()(SubscribeDone subscribeDone) const {
      XLOG(INFO) << "SubscribeDone subID=" << subscribeDone.subscribeID;
    }

    virtual void operator()(TrackStatusRequest trackStatusRequest) const {
      XLOG(INFO) << "Subscribe ftn="
                 << trackStatusRequest.fullTrackName.trackNamespace
                 << trackStatusRequest.fullTrackName.trackName;
    }
    virtual void operator()(TrackStatus trackStatus) const {
      XLOG(INFO) << "Subscribe ftn=" << trackStatus.fullTrackName.trackNamespace
                 << trackStatus.fullTrackName.trackName;
    }
    virtual void operator()(Goaway goaway) const {
      XLOG(INFO) << "Goaway, newURI=" << goaway.newSessionUri;
    }
  

    private:
    std::shared_ptr<MoQSession> clientSession_;
    std::shared_ptr<moxygen::MoQRelayClientAk::MoQControlMessageHandler> controlMessageHandler_;
    folly::EventBase* evb_;
    MoQRelayClientAk& client_;

    };


  // folly::coro::Task<void> run(
  //     Role role,
  //     std::vector<moxygen::SubscribeRequest> subs,
  //     std::chrono::milliseconds connectTimeout = std::chrono::seconds(60),
  //     std::chrono::milliseconds transactionTimeout = std::chrono::seconds(200)) {
  //   try {
  //     XLOG(INFO) << "running";
  //     co_await moqClient_.setupMoQSession(
  //         connectTimeout, transactionTimeout, role);
  //     auto exec = co_await folly::coro::co_current_executor;
  //     // auto controller = controllerFn_(moqClient_.moqSession_);
  //     auto controller = std::make_unique<RelayClientControlVisitor>(moqClient_.moqSession_);

  //     if (!controller) {
  //       XLOG(ERR) << "Failed to make controller";
  //       sessionContract_.first.setException(std::runtime_error("Failed to make controller"));
  //       co_return; // folly::makeUnexpected(MoQClientError({-1, "Failed to make controller"}));
  //     }
  //     controlReadLoop(std::move(controller)).scheduleOn(exec).start();
  //     // could parallelize
  //     if (!moqClient_.moqSession_) {
  //       XLOG(ERR) << "Session is dead now #sad";
  //       sessionContract_.first.setException(std::runtime_error("Session is dead now"));
  //       co_return; // folly::makeUnexpected(MoQClientError({-2, "Session is dead now"}));
  //     }
  //   } catch (const std::exception& ex) {
  //     XLOG(ERR) << ex.what();
  //     sessionContract_.first.setException(ex);
  //     co_return; // folly::makeUnexpected(MoQClientError({-3, ex.what()}));
  //   }
  //   // auto shared_session = moqClient_.moqSession_;
  //   // co_return shared_session;
  //   sessionContract_.first.setValue(moqClient_.moqSession_);
  // }


  folly::coro::Task<folly::Expected<std::shared_ptr<MoQSession>, MoQClientError>> run(
      Role role,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(60),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(200)) {
    try {
      XLOG(INFO) << "running MoQRelayClientAk";
      auto moqClient = std::make_shared<MoQClient>(evb_, url_); // MoQClient(evb_, url_);
      co_await moqClient->setupMoQSession(
          connectTimeout, transactionTimeout, role);

      //todo: add a cancellation token here
      controlReadLoop(moqClient->moqSession_).scheduleOn(evb_).start();
      // could parallelize
      if (!moqClient->moqSession_) {
        XLOG(ERR) << "Session is dead now #sad";
        co_return folly::makeUnexpected(MoQClientError({-2, "Session is dead now"}));
      }

      //add moqClient to the map
      auto session = moqClient->moqSession_;
      // controlMessageHandler_.addClient(std::move(moqClient), session);
      controlMessageHandler_->data_->clients_.insert({session, std::move(moqClient)});
      XLOG(INFO) << "adding client" << " clients size = " << controlMessageHandler_->data_->clients_.size();
      co_return session;

    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      // sessionContract_.first.setException(ex);
       XLOG(ERR) << ex.what() << " 2";
      co_return folly::makeUnexpected(MoQClientError({-3, ex.what()}));
    }
  }

 
  void addDownstreamSession(std::shared_ptr<MoQSession> upstreamsession, std::shared_ptr<MoQSession> downstreamSession);
  void removeSessionFromDownstreamMap(std::shared_ptr<MoQSession> session);
  void addUpstreamSessionTracknamespace(std::shared_ptr<MoQSession> upstreamSession, std::string trackNamespace);
  void removeUpstreamSessionTracknamespace(std::shared_ptr<MoQSession> session);
  void removeSessionFromData(std::shared_ptr<MoQSession> session);
  

//  private:
  folly::coro::Task<void> controlReadLoop(std::shared_ptr<MoQSession> session) {
    auto controller = makeControlVisitor(session);
    while (session) {
      auto msg = co_await session->controlMessages().next();
      if (!msg) {
        break;
      }
      XLOG(INFO) << "Got control message"<<"about to apply controller";
      boost::apply_visitor(*controller, msg.value());
      XLOG(INFO) << "Applied controller";
    }
  }

  std::unique_ptr<MoQSession::ControlVisitor> makeControlVisitor(std::shared_ptr<MoQSession> session) {
    auto controller = std::make_unique<MoQRelayClientAk::RelayClientControlVisitor>(session, controlMessageHandler_, evb_, *this);
    return controller;
  }

  folly::coro::Task<void> onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session){
    XLOG(INFO) << "MoQRelayClientAk onUnannounce: " << unAnn.trackNamespace;

    auto it = announceMap_->find(unAnn.trackNamespace);
    if (it != announceMap_->end()) {
      announceMap_->erase(it);
      // first_relay_.erase(unAnn.trackNamespace);
    }

    // for (auto it = subscriptions_->begin(); it != subscriptions_->end();) {
    //   if (it->first.trackNamespace == unAnn.trackNamespace) {
        
    //     auto subscription = it->second;
    //     subscription.cancellationSource.requestCancellation();

    //     //send unannounce to subscribers
    //     subscription.forwarder->forwardUnannounce(unAnn);
        
    //     XLOG(INFO) << "removing from subscriptions, now len is: " << subscriptions_->size();
    //     it = subscriptions_->erase(it);
    //     XLOG(INFO) << "removed from subscriptions, now len is: " << subscriptions_->size();
    //   } else {
    //     it++;
    //   }
    // }

    XLOG(INFO) << "Removing from database";
    auto harperdb = moxygen::HarperDBQuery(session->getEventBase());
    co_await harperdb.executeDeleteQuery(unAnn.trackNamespace, true);
    XLOG(INFO) << "removed from database";
  }


  // MoQClient moqClient_;
  // std::function<std::unique_ptr<MoQSession::ControlVisitor>(std::shared_ptr<MoQSession>)> controllerFn_;
  folly::EventBase* evb_{nullptr};
  proxygen::URL url_;

  std::shared_ptr< folly::F14FastMap<std::string, std::shared_ptr<MoQSession>> > announceMap_;
  // std::shared_ptr<folly::F14FastMap<FullTrackName, moxygen::MoQRelayAk::RelaySubscription, FullTrackName::hash> > subscriptions_;

  std::shared_ptr<MoQSessionData> sessionData_ = std::make_shared<MoQSessionData>();
  std::shared_ptr<MoQControlMessageHandler> controlMessageHandler_ = std::make_shared<MoQControlMessageHandler>(sessionData_);

  //unordered map of client session and its control visitor
  // std::unordered_map<std::shared_ptr<MoQSession>, std::shared_ptr<MoQRelayClientAk::RelayClientControlVisitor>> controlVisitors_;

};

} // namespace moxygen
