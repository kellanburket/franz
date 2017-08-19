//
//  KafkaRequest.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class KafkaRequest {

    static private var _correlationIdIndex: Int32 = 0

    private var _apiKey: Int16
    private var _apiVersion: Int16
    private var _correlationId: Int32
    private var value: KafkaType?
	var clientId: String?

    var correlationId: Int32 {
        return _correlationId
    }

    var message: KafkaType? {
        return value
    }

    init(apiKey: ApiKey, value: KafkaType? = nil, apiVersion: ApiVersion = .defaultVersion) {
		KafkaRequest._correlationIdIndex += 1
        self._correlationId = Int32(KafkaRequest._correlationIdIndex)
        self._apiKey = Int16(apiKey.rawValue)
        self._apiVersion = Int16(apiVersion.rawValue)
        self.value = value
    }

    var headerLength: Int {
        return _apiKey.dataLength +
            _apiVersion.dataLength +
            _correlationId.dataLength +
            clientId.dataLength
    }

    var dataLength: Int {
        return headerLength + (value?.dataLength ?? 0)
    }

    var sizeDataLength: Int {
        return 4
    }

    var sizeData: Data {
        return Int32(self.dataLength).data
    }

    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)

        data.append(self.sizeData)
        data.append(self._apiKey.data)
        data.append(self._apiVersion.data)
        data.append(self.correlationId.data)
        data.append(self.clientId.data)

        if let value = self.value {
            data.append(value.data)
        }

        //print("REQUEST LENGTH: \(data.length)")
        //print(self.description)
        return data
    }()
}

