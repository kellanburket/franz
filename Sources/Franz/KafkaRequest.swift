//
//  KafkaRequest.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class KafkaRequest: NSObject {

    static fileprivate var _correlationIdIndex: Int32 = 0
    
    fileprivate var _apiKey: KafkaInt16
    fileprivate var _apiVersion: KafkaInt16
    fileprivate var _clientId: KafkaString?
    fileprivate var _correlationId: KafkaInt32
    fileprivate var value: KafkaClass?
    
    var clientId: KafkaString {
        get {
            return _clientId ?? KafkaString(value: "")
        }

        set(newString) {
            _clientId = newString
        }
    }
    
    var correlationId: Int32 {
        return _correlationId.value
    }
    
    var message: KafkaClass? {
        return value
    }
    
    init(
        apiKey: ApiKey,
        value: KafkaClass? = nil,
        apiVersion: ApiVersion = ApiVersion.defaultVersion
    ) {
		KafkaRequest._correlationIdIndex += 1
        self._correlationId = KafkaInt32(value: KafkaRequest._correlationIdIndex)
        self._apiKey = KafkaInt16(value: apiKey.rawValue)
        self._apiVersion = KafkaInt16(value: apiVersion.rawValue)
        self.value = value
        super.init()
    }
    
    var headerLength: Int {
        return _apiKey.length +
            _apiVersion.length +
            _correlationId.length +
            clientId.length
    }
    
    var length: Int {
        return headerLength + (value?.length ?? 0)
    }
    
    var sizeDataLength: Int {
        return 4
    }
    
    var sizeData: Data {
        return Int32(self.length).data as Data
    }
    
    lazy var data: Data = {
        let data = NSMutableData(capacity: self.length)!
        
        data.append(self.sizeData)
        data.append(self._apiKey.data as Data)
        data.append(self._apiVersion.data as Data)
        data.append(self.correlationId.data as Data)
        data.append(self.clientId.data as Data)
        
        if let value = self.value {
            data.append(value.data as Data)
        }
        
        //print("REQUEST LENGTH: \(data.length)")
        //print(self.description)
        return data as Data
    }()
    
    override var description: String {
        let value = self.value?.description ?? String()
        
        return "REQUEST(\(length)):\n" +
            "\tSIZE(\(sizeDataLength)): \(sizeData)\n" +
            "\tAPI_KEY(\(_apiKey.length)): \(_apiKey.data)\n" +
            "\tAPI_VERSION(\(_apiVersion.length)): \(_apiVersion.data)\n" +
            "\tCORRELATION_ID(\(_correlationId.length)): \(correlationId.data)\n" +
            "\tCLIENT_ID(\(clientId.length)): \(_clientId?.value) => \(_clientId?.data)\n" +
            value
    }
}
