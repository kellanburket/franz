//
//  Group.swift
//  Pods
//
//  Created by Kellan Cummings on 1/26/16.
//
//

import Foundation


public class Group {
    private var _broker: Broker
    private var _clientId: String
    private var _groupProtocol: GroupProtocol
    private var _groupId: String
    private var _generationId: Int32
    private var _version: ApiVersion = ApiVersion.DefaultVersion

    private var _state: GroupState?

    public var groupProtocol: String {
        return _groupProtocol.value
    }

    public var generationId: Int32 {
        return _generationId
    }
    
    public var id: String {
        return _groupId
    }
    
    internal init(
        broker: Broker,
        clientId: String,
        groupProtocol: GroupProtocol,
        groupId: String,
        generationId: Int32
    ) {
        _broker = broker
        _clientId = clientId
        _groupProtocol = groupProtocol
        _groupId = groupId
        _generationId = generationId
    }
    
    public func getState(callback: (String, GroupState) -> ()) {
        _broker.describeGroups(self.id, clientId: _clientId) { id, state in
            self._state = state
            callback(id, state)
        }
    }
}


public class ConsumerGroup: Group {
    internal init(
        broker: Broker,
        clientId: String,
        groupId: String,
        generationId: Int32
    ) {
        super.init(
            broker: broker,
            clientId: clientId,
            groupProtocol: GroupProtocol.Consumer,
            groupId: groupId,
            generationId: generationId
        )
    }
}


public struct GroupMembership {
    var _group: Group
    var _memberId: String
    
    public var group: Group {
        return _group
    }
    
    public var memberId: String {
        return _memberId
    }
    
    init(group: Group, memberId: String) {
        self._group = group
        self._memberId = memberId
    }

    public func sync(
        topics: [String: [Int32]],
        callback: (() -> ())? = nil
    ) {
        sync(topics, data: NSData(), callback: callback)
    }

    public func sync(
        topics: [String: [Int32]],
        data: NSData = NSData(),
        callback: (() -> ())? = nil
    ) {
        group._broker.syncConsumerGroup(
            group.id,
            generationId: group.generationId,
            memberId: memberId,
            topics: topics,
            userData: data,
            clientId: group._clientId,
            version: group._version,
            callback: callback
        )
    }
    
    public func leave() {
        
    }
    
    public func heartbeat() {
        
    }
}
