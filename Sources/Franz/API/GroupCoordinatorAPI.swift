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


class GroupCoordinatorRequestMessage: KafkaType {
    
    private var _groupId: String
    
    init(groupId: String) {
        _groupId = groupId
    }
    
	required init(data: inout Data) {
        _groupId = String(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._groupId.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._groupId.data)
        return data
    }()
    
    var id: String {
		return _groupId 
    }
}


class GroupCoordinatorResponse: KafkaResponse {
    
    var _errorCode: Int16
    var _coordinatorId: Int32
    var _coordinatorHost: String
    var _coordinatorPort: Int32
    
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: _errorCode)
    }
    
    var id: Int32 {
        return _coordinatorId
    }
    
    var host: String {
		return _coordinatorHost 
    }

    var port: Int32 {
        return _coordinatorPort
    }
	
	//TODO: Convert to struct
	init(errorCode: Int16, coordinatorId: Int32, coordinatorHost: String, coordinatorPort: Int32) {
		_errorCode = errorCode
		_coordinatorId = coordinatorId
		_coordinatorHost = coordinatorHost
		_coordinatorPort = coordinatorPort
	}
    
	required init(data: inout Data) {
        _errorCode = Int16(data: &data)
        _coordinatorId = Int32(data: &data)
        _coordinatorHost = String(data: &data)
        _coordinatorPort = Int32(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength +
            self._coordinatorId.dataLength +
            self._coordinatorHost.dataLength +
            self._coordinatorPort.dataLength
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        data.append(self._errorCode.data)
        data.append(self._coordinatorId.data)
        data.append(self._coordinatorHost.data)
        data.append(self._coordinatorPort.data)
        return data
    }()
}
