#include <folly/init/Init.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <folly/io/async/EventBase.h>

#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>

#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <folly/FBString.h>

// #include <nlohmann/json.hpp>
// using json = nlohmann::json;

#pragma once

namespace moxygen {


const std:: string RELAY_ID = "3";
const double RELAY_LAT =  50.11552;
const double RELAY_LON =  8.68417;

const std::string DB_URL = "http://172-236-78-145.ip.linodeusercontent.com:9925/";

class HarperDBConnector : public proxygen::HTTPConnector::Callback {

public:
    HarperDBConnector(folly::EventBase* evb) :
        evb_(evb) {
        XLOG(INFO) << "HarperDB";
    }

    ~HarperDBConnector() {
        XLOG(INFO) << "~HarperDB";
    }

    class HarperHTTPHandler : public proxygen::HTTPTransactionHandler {

    public:

        explicit HarperHTTPHandler(HarperDBConnector& db) : db_(db) {
            XLOG(INFO) << "HarperHTTPHandler";
        }

        ~HarperHTTPHandler() {
            XLOG(INFO) << "~HarperHTTPHandler";
        }

        void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
            txn_ = txn;
        }

        void detachTransaction() noexcept override {
            XLOG(INFO) << "detachTransaction";
            txn_ = nullptr;
        }

        void onHeadersComplete(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override {
            XLOG(INFO) << "onHeadersComplete";
        }

        void onBody(std::unique_ptr<folly::IOBuf> resp) noexcept override {
            XLOG(INFO) << "Body: " ;
            // XLOG(INFO) << resp->moveToFbString().toStdString();
            // XLOG(INFO) << resp->moveToFbString().toStdString();
            
            resp_.append(std::move(resp));
        }

        void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override {
            XLOG(INFO) << "onUpgrade";
        }

        void onTrailers(std::unique_ptr<proxygen::HTTPHeaders>) noexcept override {
            XLOG(INFO) << "onTrailers";
        }

        void onEgressPaused() noexcept override {
            XLOG(INFO) << "onEgressPaused";
        }

        void onEgressResumed() noexcept override {
            XLOG(INFO) << "onEgressResumed";
        }

        void onEOM() noexcept override {
            XLOG(INFO) << "onEOM";
            std::string collectedBody;
            auto combinedBuffer = resp_.move();

            folly::fbstring result(combinedBuffer->moveToFbString());
            collectedBody = result.toStdString();

            XLOG(INFO) << "EOM: " << collectedBody;
            
            if (txn_) {
                txn_->sendAbort();
            }

            responseContract_.first.setValue(collectedBody);

            eomContract_.first.setValue(0);
        }

        void onError(const proxygen::HTTPException& ex) noexcept override {
            XLOG(INFO) << "onError: " << ex.what();
        }

        HarperDBConnector& db_;
        proxygen::HTTPTransaction* txn_{nullptr};
        folly::IOBufQueue resp_{folly::IOBufQueue::cacheChainLength()};
        std::pair<
            folly::coro::Promise<std::string>,
            folly::coro::Future<std::string>>
                responseContract_{
                    folly::coro::makePromiseContract<std::string>()};

        std::pair<
            folly::coro::Promise<int>,
            folly::coro::Future<int>>
        eomContract_{
            folly::coro::makePromiseContract<int>()};
    };

    void connectSuccess(proxygen::HTTPUpstreamSession* session) noexcept override {
        XLOG(INFO) << "connectSuccess";
        // session_ = session;

        sessionContract.first.setValue(session);

        // proxygen::HTTPMessage req;
        // req.setMethod(proxygen::HTTPMethod::GET);
        // req.setURL("/RelayLocation/");
        // req.getHeaders().add("Authorization", "Basic SERCX0FETUlOOnBhc3N3b3Jk");

        // auto txn = session->newTransaction(&httpHandler_);
        // txn->sendHeaders(req);
        // txn->sendEOM();

        // XLOG(INFO) << "connectSuccess done";
    }

    void connectError(const folly::AsyncSocketException& ex) noexcept override {
        XLOG(ERR) << "Connect error: " << ex.what();
    }

    std::pair<
        folly::coro::Promise<proxygen::HTTPUpstreamSession*>,
        folly::coro::Future<proxygen::HTTPUpstreamSession*>>
        sessionContract{
            folly::coro::makePromiseContract<proxygen::HTTPUpstreamSession*>()};

    HarperHTTPHandler httpHandler_{*this};
    proxygen::HTTPUpstreamSession* session_{nullptr};  

private:
    folly::EventBase* evb_{nullptr};
    folly::StringPiece dburl_{"http://172-236-78-145.ip.linodeusercontent.com:9926/RelayLocation/"};
};

}
