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
        super.init(apiKey: ApiKey.groupCoordinatorRequest, value: value)
    }
    
}


class GroupCoordinatorRequestMessage: KafkaClass {
    
    fileprivate var _groupId: KafkaString
    
    init(groupId: String) {
        _groupId = KafkaString(value: groupId)
    }
    
	required init( bytes: inout [UInt8]) {
        _groupId = KafkaString(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._groupId.length
    }()
    
    lazy var data: Data = {
        var data = NSMutableData(capacity: self.length)!
        data.append(self._groupId.data as Data)
        return data as Data
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
    
	required init( bytes: inout [UInt8]) {
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
    
    lazy var data: Data = {
        let data = NSMutableData(capacity: self.length)!
        data.append(self._errorCode.data as Data)
        data.append(self._coordinatorId.data as Data)
        data.append(self._coordinatorHost.data as Data)
        data.append(self._coordinatorPort.data as Data)
        return data as Data
    }()
    
    override var description: String {
        return "ERROR CODE: \(self.error?.code ?? 0)\n" +
            "ERROR DESCRIPTION: \(self.error?.description ?? String())\n" +
            "COORDINATOR ID(\(self._coordinatorId.length)): \(id) => \(self._coordinatorId.data)\n" +
            "COORDINATOR HOST(\(self._coordinatorHost.length)): \(host) => \(self._coordinatorHost.data)\n" +
            "COORDINATOR PORT(\(self._coordinatorPort.length)): \(port) => \(self._coordinatorPort.data)\n"
    }
}
