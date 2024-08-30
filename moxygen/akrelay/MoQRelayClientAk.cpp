#include "MoQRelayClientAk.h"

#include <folly/logging/xlog.h>
#include <folly/lang/Exception.h>
#include <folly/coro/Task.h>

namespace moxygen {

    folly::coro::Task<void> MoQRelayClientAk::MoQControlMessageHandler::onUnannounce(Unannounce unAnn, std::shared_ptr<MoQSession> session)
    {
        XLOG(DBG1) << __func__ << unAnn.trackNamespace;
        auto it = data_.downstreamSessions_.find(session);
        if(it != data_.downstreamSessions_.end()){
            for(auto& downstreamSession : it->second){
                downstreamSession->unannounce(unAnn);
            }
        }

        co_return;
    }

    void MoQRelayClientAk::MoQControlMessageHandler::addClient(std::shared_ptr<MoQClient> moqClient, std::shared_ptr<MoQSession> session){
        data_.clients_.emplace(std::move(session), std::move(moqClient));
        // data_.clients_.emplace(
        //     std::piecewise_construct,
        //     std::forward_as_tuple(std::move(session)),
        //     std::forward_as_tuple(std::move(moqClient))
        // );
        XLOG(INFO) << __func__ << " clients=" << data_.clients_.size();
    }

    void MoQRelayClientAk::addDownstreamSession(std::shared_ptr<MoQSession> upstreamsession, std::shared_ptr<MoQSession> downstreamSession){
        controlMessageHandler_.data_.downstreamSessions_[upstreamsession].insert(std::move(downstreamSession));
        XLOG(INFO) << __func__ << " sessions size=" << controlMessageHandler_.data_.downstreamSessions_.size();
    }

    // void MoQRelayClientAk::removeDownstreamSession(std::shared_ptr<MoQSession> upstreamsession, std::shared_ptr<MoQSession> downstreamSession){
    //     this->controlMessageHandler_.data_.downstreamSessions_[upstreamsession].erase(std::move(downstreamSession));
    //     //erase if set empty
    // }

    void MoQRelayClientAk::addUpstreamSessionTracknamespace(std::shared_ptr<MoQSession> upstreamSession, std::string trackNamespace){
        controlMessageHandler_.data_.upstreamSessionTracknamespace_[upstreamSession] = std::move(trackNamespace);
        XLOG(INFO) << __func__ << " session tracks size=" << controlMessageHandler_.data_.upstreamSessionTracknamespace_.size();
    }


}