//
//  KafkaRequest.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class KafkaRequest: NSObject {

    static private var _correlationIdIndex: Int32 = 0
    
    private var _apiKey: KafkaInt16
    private var _apiVersion: KafkaInt16
    private var _clientId: KafkaString?
    private var _correlationId: KafkaInt32
    private var value: KafkaClass?
    
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
        apiVersion: ApiVersion = ApiVersion.DefaultVersion
    ) {
        self._correlationId = KafkaInt32(value: ++KafkaRequest._correlationIdIndex)
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
    
    var sizeData: NSData {
        return Int32(self.length).data
    }
    
    lazy var data: NSData = {
        let data = NSMutableData(capacity: self.length)!
        
        data.appendData(self.sizeData)
        data.appendData(self._apiKey.data)
        data.appendData(self._apiVersion.data)
        data.appendData(self.correlationId.data)
        data.appendData(self.clientId.data)
        
        if let value = self.value {
            data.appendData(value.data)
        }
        
        //print("REQUEST LENGTH: \(data.length)")
        //print(self.description)
        return data
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