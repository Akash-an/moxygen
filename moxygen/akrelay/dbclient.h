#include <folly/init/Init.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <folly/io/async/EventBase.h>

#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>

#pragma once

namespace moxygen {


const std:: string RELAY_ID = "1";
// const folly::StringPiece DB_URL{"http://172-236-78-145.ip.linodeusercontent.com:9925/"};
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
            XLOG(INFO) << resp->moveToFbString().toStdString();
            responseContract_.first.setValue(std::move(resp));
            /*this has to be written like below. it can be called multiple times*/
            /*
                void CurlClient::onBody(std::unique_ptr<folly::IOBuf> chain) noexcept {
                    if (onBodyFunc_ && chain) {
                        onBodyFunc_.value()(request_, chain.get());
                    }
                    if (!loggingEnabled_) {
                        return;
                    }
                    CHECK(outputStream_);
                    if (chain) {
                        const IOBuf* p = chain.get();
                        do {
                        outputStream_->write((const char*)p->data(), p->length());
                        outputStream_->flush();
                        p = p->next();
                        } while (p != chain.get());
                    }
                }
            */
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
            if (txn_) {
                txn_->sendAbort();
            }

            eomContract_.first.setValue(0);
        }

        void onError(const proxygen::HTTPException& ex) noexcept override {
            XLOG(INFO) << "onError: " << ex.what();
        }

        HarperDBConnector& db_;
        proxygen::HTTPTransaction* txn_{nullptr};
        std::unique_ptr<folly::IOBuf> resp_{nullptr};
        std::pair<
        folly::coro::Promise<std::unique_ptr<folly::IOBuf>>,
        folly::coro::Future<std::unique_ptr<folly::IOBuf>>>
        responseContract_{
            folly::coro::makePromiseContract<std::unique_ptr<folly::IOBuf>>()};

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
