#include <string>

#include <proxygen/lib/http/session/HTTPUpstreamSession.h>

#include <folly/logging/xlog.h>
#include <folly/io/IOBuf.h>

#include "RelayGlobalVars.h"


namespace moxygen{

    class HarperDBQuery{
    public:
        HarperDBQuery(folly::EventBase* evb): evb_(evb){
            XLOG(INFO) << "HarperDBQuery";

        }
        
        virtual ~HarperDBQuery(){}



        folly::coro::Task<void> setHarperDBConnector(){
            XLOG(INFO) << "setHarperDBConnector";
            auto dbconnector = std::make_unique<moxygen::HarperDBConnector>(evb_);
            auto httpConnector = std::make_unique<proxygen::HTTPConnector> (&(*dbconnector), proxygen::WheelTimerInstance(std::chrono::milliseconds(5000)));
            folly::StringPiece url_sp(moxygen::DB_URL);
            proxygen::URL url{url_sp};
            httpConnector->connect(evb_,folly::SocketAddress(
                        url.getHost(), url.getPort(), true),std::chrono::milliseconds(10000));
            dbconnector_ = std::move(dbconnector);
            auto session_ftr = co_await co_awaitTry(std::move(dbconnector_->sessionContract.second));

            session_ = std::move(session_ftr.value());

        }

        
        folly::coro::Task<void> executeInsertQuery(std::string tracknamespace, bool originalPublisher){
            XLOG(INFO) << "executeInsertQuery";
            co_await setHarperDBConnector();
            
            txn_ = session_->newTransaction(&dbconnector_->httpHandler_);

            // auto wt = txn_->getTransport();
            std::string full_url = moxygen::DB_URL;// + "application-template/insertAnnounce";
            folly::StringPiece full_url_sp(full_url);
            proxygen::URL url(full_url_sp);
            proxygen::HTTPMessage req;
            XLOG(INFO) << "sending request for " << full_url;
            req.setMethod(proxygen::HTTPMethod::POST);
            req.setURL(url.makeRelativeURL());
            req.getHeaders().add("Authorization", "Basic SERCX0FETUlOOnBhc3N3b3Jk");
            req.getHeaders().add("Content-Type", "application/json");

            // std::string query = "insert into data.Announces (tracknamespace, relayid, orignalPublisher) values('vc','1', true)";
            std::string originalPublisherStr = originalPublisher ? "true" : "false";
            std::string query = "insert into data.Announces (tracknamespace, relayid, originalpublisher) values('" + tracknamespace + "','" + moxygen::RELAY_ID + "'," + originalPublisherStr + ")";
            // {
            //     "operation": "sql",
            //     "sql": "insert into data.Announces (id, tracknamespace,relayid) values('296', 'abcd', '55')"
            // }
            std::string json_body = "{\"operation\":\"sql\",\"sql\":\"" + query + "\"}";

            auto req_body = folly::IOBuf::copyBuffer(json_body);

            auto content_length = req_body->computeChainDataLength();
            req.getHeaders().add("Content-Length", std::to_string(content_length));

            XLOG(INFO) << json_body;
            txn_->sendHeaders(req);
            txn_->sendBody(std::move(req_body));
            // session_->sendBody(txn_, std::move(req_body), true, true);
            txn_->sendEOM();
            XLOG(INFO) << "sent eom";


            auto resp_ftr = co_await co_awaitTry(std::move(dbconnector_->httpHandler_.responseContract_.second));
            auto resp = std::move(resp_ftr.value());
            XLOG(INFO) << resp;
            auto x = co_await co_awaitTry(std::move(dbconnector_->httpHandler_.eomContract_.second));
        }


    folly::coro::Task<void> executeDeleteQuery(std::string tracknamespace, bool originalPublisher){
        XLOG(INFO) << "executeDeleteQuery";
        co_await setHarperDBConnector();
        txn_ = session_->newTransaction(&dbconnector_->httpHandler_);
        std::string full_url = moxygen::DB_URL;
        folly::StringPiece full_url_sp(full_url);
        proxygen::URL url(full_url_sp);
        proxygen::HTTPMessage req;
        XLOG(INFO) << "sending request for " << full_url;
        req.setMethod(proxygen::HTTPMethod::POST);
        req.setURL(url.makeRelativeURL());
        req.getHeaders().add("Authorization", "Basic SERCX0FETUlOOnBhc3N3b3Jk");
        req.getHeaders().add("Content-Type", "application/json");

    
        std::string whereClause = originalPublisher ? "" : " and originalpublisher = false";
        std::string query = "delete from data.Announces where tracknamespace = '" + tracknamespace + "' and relayid = '" + moxygen::RELAY_ID + "'" + whereClause;
        std::string json_body = "{\"operation\":\"sql\",\"sql\":\"" + query + "\"}";

        auto req_body = folly::IOBuf::copyBuffer(json_body);

        auto content_length = req_body->computeChainDataLength();
        req.getHeaders().add("Content-Length", std::to_string(content_length));

        XLOG(INFO) << json_body;
        txn_->sendHeaders(req);
        txn_->sendBody(std::move(req_body));
        // session_->sendBody(txn_, std::move(req_body), true, true);
        txn_->sendEOM();
        XLOG(INFO) << "sent eom";
        auto resp_ftr = co_await co_awaitTry(std::move(dbconnector_->httpHandler_.responseContract_.second));
        auto resp = std::move(resp_ftr.value());
        XLOG(INFO) << "response: " << resp;
        
        // auto x = co_await co_awaitTry(std::move(dbconnector_->httpHandler_.eomContract_.second));
    }

    folly::coro::Task<std::string> getNearestRelay(std::string tracknamespace) {
        XLOG(INFO) << "getNearestRelay";

        co_await setHarperDBConnector();
        txn_ = session_->newTransaction(&dbconnector_->httpHandler_);

        std::string full_url = moxygen::DB_URL;
        folly::StringPiece full_url_sp(full_url);
        proxygen::URL url(full_url_sp);
        proxygen::HTTPMessage req;
        XLOG(INFO) << "sending request for " << full_url;
        req.setMethod(proxygen::HTTPMethod::POST);
        req.setURL(url.makeRelativeURL());
        req.getHeaders().add("Authorization", "Basic SERCX0FETUlOOnBhc3N3b3Jk");
        req.getHeaders().add("Content-Type", "application/json");
        
        // select r.id, r.hostname, r.zone, geodistance('[77.59369,12.97194]', geo) as distance from data.RelayLocation r inner join data.Announces a on r.id = a.relayid and a.tracknamespace='vc' and r.id!='3' ORDER BY distance ASC limit 1
        std::string query = "select r.id, r.hostname, r.zone, geodistance('[" + std::to_string(moxygen::RELAY_LON) + ", " + std::to_string(moxygen::RELAY_LAT) 
                                + "]', geo) as distance from data.RelayLocation r inner join data.Announces a on r.id = a.relayid and a.tracknamespace='" 
                                + tracknamespace + "' and r.id!='" + moxygen::RELAY_ID + "' ORDER BY distance ASC limit 1"; 
        
        std::string json_body = "{\"operation\":\"sql\",\"sql\":\"" + query + "\"}";
        auto req_body = folly::IOBuf::copyBuffer(json_body);
        
        auto content_length = req_body->computeChainDataLength();
        req.getHeaders().add("Content-Length", std::to_string(content_length));
        XLOG(INFO) << json_body;
        txn_->sendHeaders(req);
        txn_->sendBody(std::move(req_body));
        txn_->sendEOM();
        XLOG(INFO) << "sent eom";


        auto resp_ftr = co_await co_awaitTry(std::move(dbconnector_->httpHandler_.responseContract_.second));
        auto resp = std::move(resp_ftr.value());
        // XLOG(INFO) << resp->moveToFbString().toStdString();
        // auto x = co_await co_awaitTry(std::move(dbconnector_->httpHandler_.eomContract_.second));
        std::string hostname = extractValue(resp, "hostname");
        XLOG(INFO) << "hostname: " << hostname;
        co_return hostname;
    }

    std::string extractValue(const std::string& json, const std::string& key) {
        std::string search_key = "\"" + key + "\":";
        size_t key_pos = json.find(search_key);
        if (key_pos == std::string::npos) {
            return "";
        }

        size_t start_pos = json.find("\"", key_pos + search_key.length()) + 1;
        size_t end_pos = json.find("\"", start_pos);
        return json.substr(start_pos, end_pos - start_pos);
    }

    private:
        std::unique_ptr<moxygen::HarperDBConnector> dbconnector_{nullptr};
        proxygen::HTTPUpstreamSession* session_{nullptr};
        folly::EventBase* evb_;
        proxygen::HTTPTransaction* txn_{nullptr};

    };
}