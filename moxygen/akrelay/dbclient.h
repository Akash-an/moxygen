#include <folly/init/Init.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <folly/io/async/EventBase.h>

#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>

namespace moxygen {

class HarperDB : public proxygen::HTTPConnector::Callback {

public:
    HarperDB(folly::EventBase* evb) :
        evb_(evb) {
        XLOG(INFO) << "HarperDB";
    }

    ~HarperDB() {
        XLOG(INFO) << "~HarperDB";
    }

    class HarperHTTPHandler : public proxygen::HTTPTransactionHandler {

    public:

        explicit HarperHTTPHandler(HarperDB& db) : db_(db) {
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
        }

        void onError(const proxygen::HTTPException& ex) noexcept override {
            XLOG(INFO) << "onError: " << ex.what();
        }

        HarperDB& db_;
        proxygen::HTTPTransaction* txn_{nullptr};
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

    void getNearestRelay() {
        // folly::StringPiece dburl("https://172-236-78-145.ip.linodeusercontent.com:4433/moq");
    }

    std::pair<
        folly::coro::Promise<proxygen::HTTPUpstreamSession*>,
        folly::coro::Future<proxygen::HTTPUpstreamSession*>>
        sessionContract{
            folly::coro::makePromiseContract<proxygen::HTTPUpstreamSession*>()};

    HarperHTTPHandler httpHandler_{*this};
private:
    folly::EventBase* evb_{nullptr};
    proxygen::HTTPUpstreamSession* session_{nullptr};
    
    folly::StringPiece dburl_{"http://172-236-78-145.ip.linodeusercontent.com:9926/RelayLocation/"};
};

}
