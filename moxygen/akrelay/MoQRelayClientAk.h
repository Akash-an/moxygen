/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include <folly/lang/Exception.h>

#include "moxygen/MoQClient.h"

namespace moxygen {

class MoQRelayClientAk {
 public:
  MoQRelayClientAk(
      folly::EventBase* evb,
      proxygen::URL url)
      : moqClient_(evb, url) {}

  ~MoQRelayClientAk() = default;

  struct MoQClientError{
    int code;
    std::string message;
  };
  

    class RelayClientControlVisitor : public MoQSession::ControlVisitor {
    public:
      explicit RelayClientControlVisitor(
          std::shared_ptr<MoQSession> clientSession)
          : clientSession_(std::move(clientSession)) {}

      ~RelayClientControlVisitor() override = default;

      virtual void operator()(Announce announce) const  override{
        XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
      }

      virtual void operator()(SubscribeOk subscribeOk) const {
        XLOG(INFO) << "SubscribeOk id=" << subscribeOk.subscribeID;
        //resolve a promise here.
      }
    

      private:
      std::shared_ptr<MoQSession> clientSession_;

    };


  folly::coro::Task<void> run(
      Role role,
      std::vector<moxygen::SubscribeRequest> subs,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(5),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(60)) {
    try {
      XLOG(INFO) << "running";
      co_await moqClient_.setupMoQSession(
          connectTimeout, transactionTimeout, role);
      auto exec = co_await folly::coro::co_current_executor;
      // auto controller = controllerFn_(moqClient_.moqSession_);
      auto controller = std::make_unique<RelayClientControlVisitor>(moqClient_.moqSession_);

      if (!controller) {
        XLOG(ERR) << "Failed to make controller";
        sessionContract_.first.setException(std::runtime_error("Failed to make controller"));
        co_return; // folly::makeUnexpected(MoQClientError({-1, "Failed to make controller"}));
      }
      controlReadLoop(std::move(controller)).scheduleOn(exec).start();
      // could parallelize
      if (!moqClient_.moqSession_) {
        XLOG(ERR) << "Session is dead now #sad";
        sessionContract_.first.setException(std::runtime_error("Session is dead now"));
        co_return; // folly::makeUnexpected(MoQClientError({-2, "Session is dead now"}));
      }
      // for (auto& sub : subs) {
      //   // auto sub = SubscribeRequest{42, 0, ns, moxygen::LocationType::LatestGroup};
      //   auto res =
      //       co_await moqClient_.moqSession_->subscribe(sub);
      //   if (!res) {
      //     XLOG(ERR) << "Subscribe error id=" << res.error().subscribeID
      //               << " code=" << res.error().errorCode
      //               << " reason=" << res.error().reasonPhrase;
      //   }
      // }
    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      sessionContract_.first.setException(ex);
      co_return; // folly::makeUnexpected(MoQClientError({-3, ex.what()}));
    }
    // auto shared_session = moqClient_.moqSession_;
    // co_return shared_session;

    sessionContract_.first.setValue(moqClient_.moqSession_);

  }

  std::shared_ptr<MoQSession> getMoQSession() { return std::move(moqClient_.moqSession_); }

  std::pair<
          folly::coro::Promise<std::shared_ptr<MoQSession>>,
          folly::coro::Future<std::shared_ptr<MoQSession>>>
          sessionContract_{
              folly::coro::makePromiseContract<std::shared_ptr<MoQSession>>()};
 private:
  folly::coro::Task<void> controlReadLoop(
      std::unique_ptr<MoQSession::ControlVisitor> controller) {
    while (moqClient_.moqSession_) {
      auto msg = co_await moqClient_.moqSession_->controlMessages().next();
      if (!msg) {
        break;
      }
      XLOG(INFO) << "Got control message"<<"about to apply controller";
      boost::apply_visitor(*controller, msg.value());
      XLOG(INFO) << "Applied controller";
    }
  }

  MoQClient moqClient_;
  std::function<std::unique_ptr<MoQSession::ControlVisitor>(
      std::shared_ptr<MoQSession>)>
      controllerFn_;
  
  

};

} // namespace moxygen
