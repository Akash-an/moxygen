#include "MoQRelayClientAk.h"

#include <folly/logging/xlog.h>
#include <folly/lang/Exception.h>
#include <folly/coro/Task.h>

namespace moxygen {

    void MoQRelayClientAk::MoQControlMessageHandler::onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session)
    {
        XLOG(DBG1) << __func__ << unAnn.trackNamespace;
        auto it = data_->downstreamSessions_.find(session);
        if(it != data_->downstreamSessions_.end()){
            for(auto downstreamSession = it->second.begin(); downstreamSession != it->second.end();){
                if((*downstreamSession)) {
                    (*downstreamSession)->onUnannounce(unAnn);
                }
                downstreamSession = it->second.erase(downstreamSession);
            }
            //data erase happens in the server unannounce through removesession
            // data_->downstreamSessions_.erase(it);
        }
        // data_->clients_.erase(session);
    }

    void MoQRelayClientAk::MoQControlMessageHandler::addClient(std::shared_ptr<MoQClient> moqClient, std::shared_ptr<MoQSession> session){
        data_->clients_.insert({session, std::move(moqClient)});
        XLOG(INFO) << __func__ << " clients=" << data_->clients_.size();
    }

    void MoQRelayClientAk::addDownstreamSession(std::shared_ptr<MoQSession> upstreamsession, std::shared_ptr<MoQSession> downstreamSession){
        // controlMessageHandler_.data_.downstreamSessions_[upstreamsession].insert(std::move(downstreamSession));
        XLOG_IF(INFO, downstreamSession) << "session is not null";
        auto it = controlMessageHandler_->data_->downstreamSessions_.find(upstreamsession);
        if(it != controlMessageHandler_->data_->downstreamSessions_.end()){
            it->second.insert(std::move(downstreamSession));
        }
        else{
            auto newset = std::set<std::shared_ptr<MoQSession>>();
            newset.insert(std::move(downstreamSession));
            controlMessageHandler_->data_->downstreamSessions_.emplace(
               upstreamsession,
               newset
            );
        }
        XLOG(INFO) << __func__ << " sessions size=" << controlMessageHandler_->data_->downstreamSessions_.size();
    }

    void MoQRelayClientAk::removeSessionFromData(std::shared_ptr<MoQSession> session){
        XLOG(INFO) << __func__ << " sessions size=" << controlMessageHandler_->data_->downstreamSessions_.size();
        controlMessageHandler_->data_->downstreamSessions_.erase(session);
        XLOG(INFO) << __func__ << " sessions size=" << controlMessageHandler_->data_->downstreamSessions_.size();

        XLOG(INFO) << __func__ << " clients size=" << controlMessageHandler_->data_->clients_.size();
        controlMessageHandler_->data_->clients_.erase(session);
        XLOG(INFO) << __func__ << " clients size=" << controlMessageHandler_->data_->clients_.size();
    }

    void MoQRelayClientAk::removeDownstreamSessionFromData(std::shared_ptr<MoQSession> session){
        for(auto it = controlMessageHandler_->data_->downstreamSessions_.begin(); it != controlMessageHandler_->data_->downstreamSessions_.end();){
            it->second.erase(session);
        }
    }

    // void MoQRelayClientAk::addUpstreamSessionTracknamespace(std::shared_ptr<MoQSession> upstreamSession, std::string trackNamespace){
    //     controlMessageHandler_->data_->upstreamSessionTracknamespace_[upstreamSession] = std::move(trackNamespace);
    //     XLOG(INFO) << __func__ << " session tracks size=" << controlMessageHandler_->data_->upstreamSessionTracknamespace_.size();
    // }

    //   void MoQRelayClientAk::removeUpstreamSessionTracknamespace(std::shared_ptr<MoQSession> session){
    //       controlMessageHandler_->data_->upstreamSessionTracknamespace_.erase(session);
    //   }
}