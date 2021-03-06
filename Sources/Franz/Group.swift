//
//  Group.swift
//  Pods
//
//  Created by Kellan Cummings on 1/26/16.
//
//

import Foundation

/**
    Base class for all Client Groups.
*/
class Group {
    fileprivate var _broker: Broker
    fileprivate var _clientId: String
    fileprivate var _groupProtocol: GroupProtocol
    fileprivate var _groupId: String
    fileprivate var _generationId: Int32
    fileprivate var _version: ApiVersion = 0

    private var _state: GroupState?

    /**
        Group Protocol
    */
    var groupProtocol: String {
        return _groupProtocol.value
    }

    /**
        Generation Id
    */
    var generationId: Int32 {
        return _generationId
    }

    /**
        Group Id
    */
    var id: String {
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

    /**
        Retreive the state of the current group.
     
        - Parameter callback:   a closure which takes a group id and a state of that group as its parameters
    */
    func getState(_ callback: @escaping (String, GroupState) -> ()) {
        _broker.describeGroups(self.id, clientId: _clientId) { id, state in
            self._state = state
            callback(id, state)
        }
    }
	
	var topics = Set<TopicName>()
	internal(set) var assignedPartitions = [TopicName: [PartitionId]]()
}


/**
    A Consumer Group
*/
class ConsumerGroup: Group {
    internal init(
        broker: Broker,
        clientId: String,
        groupId: String,
        generationId: Int32
    ) {
        super.init(
            broker: broker,
            clientId: clientId,
            groupProtocol: GroupProtocol.consumer,
            groupId: groupId,
            generationId: generationId
        )
    }
}


/**
    An abstraction of a Broker's relationship to a group.
*/
class GroupMembership {
    var _group: Group
    var _memberId: String
	let members: [Member]
    
    /**
        A Group
    */
    var group: Group {
        return _group
    }

    /**
        The Broker's id
    */
    var memberId: String {
        return _memberId
    }
    
	init(group: Group, memberId: String, members: [Member]) {
        self._group = group
        self._memberId = memberId
		self.members = members
    }

    /**
        Sync the broker with the Group Coordinator
    */
    func sync(
		_ topics: [TopicName: [PartitionId]],
        data: Data = Data(),
        callback: (() -> ())? = nil
    ) {
        group._broker.syncGroup(
            group.id,
            generationId: group.generationId,
            memberId: memberId,
            topics: topics,
            userData: data,
            version: group._version) { membership in
			for assignment in membership.partitionAssignment {
				self.group.assignedPartitions[assignment.topic] = assignment.partitions.map { $0 }
			}
			self.group.topics = Set(membership.partitionAssignment.map { $0.topic })
			callback?()
		}
    }
    
    /**
        Leave the group
     
        - Parameter callback:   called when the group has successfully been left
    */
    func leave(_ callback: (() -> ())? = nil) {
        group._broker.leaveGroup(
            _group.id,
            memberId: memberId,
            clientId: _group._clientId,
            callback: callback
        )
    }

    /**
        Issue a heartbeat request.
     
        - Parameter callback:   called when the heartbeat request has successfully completed
    */
    func heartbeat(_ callback: (() -> ())? = nil) {
        group._broker.heartbeatRequest(
            _group.id,
            generationId:_group._generationId,
            memberId: memberId,
            callback: callback
        )
    }
}
