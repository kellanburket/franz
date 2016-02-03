//
//  GroupCoordinatorAPI.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class GroupCoordinatorRequest: KafkaRequest {
    
    convenience init(id: String) {
        self.init(value: GroupCoordinatorRequestMessage(groupId: id))
    }
    
    init(value: GroupCoordinatorRequestMessage) {
        super.init(apiKey: ApiKey.GroupCoordinatorRequest, value: value)
    }
    
}


class GroupCoordinatorRequestMessage: KafkaClass {
    
    private var _groupId: KafkaString
    
    init(groupId: String) {
        _groupId = KafkaString(value: groupId)
    }
    
    required init(inout bytes: [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length
    }()
    
    lazy var data: NSData = {
        var data = NSMutableData(capacity: self.length)!
        data.appendData(self._groupId.data)
        return data
    }()
    
    var id: String {
        return _groupId.value ?? String()
    }
    
    lazy var description: String = {
        return "GROUP ID(\(self._groupId.length)): \(self.id) => \(self._groupId.data)\n"
    }()
}


class GroupCoordinatorResponse: KafkaResponse {
    
    var _errorCode: KafkaInt16
    var _coordinatorId: KafkaInt32
    var _coordinatorHost: KafkaString
    var _coordinatorPort: KafkaInt32
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode.value)
    }
    
    var id: Int32 {
        return _coordinatorId.value
    }
    
    var host: String {
        return _coordinatorHost.value ?? String()
    }

    var port: Int32 {
        return _coordinatorPort.value
    }
    
    required init(inout bytes: [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _coordinatorId = KafkaInt32(bytes: &bytes)
        _coordinatorHost = KafkaString(bytes: &bytes)
        _coordinatorPort = KafkaInt32(bytes: &bytes)
        super.init(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length +
            self._coordinatorId.length +
            self._coordinatorHost.length +
            self._coordinatorPort.length
    }()
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        data.appendData(self._errorCode.data)
        data.appendData(self._coordinatorId.data)
        data.appendData(self._coordinatorHost.data)
        data.appendData(self._coordinatorPort.data)
        return data
    }()
    
    override var description: String {
        return "ERROR CODE: \(self.error?.code ?? 0)\n" +
            "ERROR DESCRIPTION: \(self.error?.description ?? String())\n" +
            "COORDINATOR ID(\(self._coordinatorId.length)): \(id) => \(self._coordinatorId.data)\n" +
            "COORDINATOR HOST(\(self._coordinatorHost.length)): \(host) => \(self._coordinatorHost.data)\n" +
            "COORDINATOR PORT(\(self._coordinatorPort.length)): \(port) => \(self._coordinatorPort.data)\n"
    }
}