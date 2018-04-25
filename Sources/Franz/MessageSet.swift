//
//  MessageSet.swift
//  Franz
//
//  Created by Kellan Cummings on 1/20/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


@objc public class MessageSet: NSObject, KafkaType {
    let values: [MessageSetItem]
	
    init(values: [MessageSetItem]) {
        self.values = values
    }

	required public init(data: inout Data) {
        var tempValues = [MessageSetItem]()
		
        while data.count > 0 {
            tempValues.append(MessageSetItem(data: &data))
        }
		
		values = tempValues
    }
    
    var dataLength: Int {
		return values.map { $0.dataLength }.reduce(0, +)
	}
    
    var data: Data {
        return values.map { $0.data }.reduce(Data(), +)
    }
}


struct MessageSetItem: KafkaType {
	
    private let value: KafkaMessage
    private let size: Int32?
	let offset: Int64
	
    var message: Message {
        return Message(data: value.value, key: value.key)
    }
    
    init(value: String, key: String? = nil, offset: Int = 0) {
        self.offset = Int64(offset)
        self.value = KafkaMessage(value: value, key: key)
		self.size = nil
    }

    init(data: Data, key: Data? = nil, offset: Int = 0) {
        self.offset = Int64(offset)
        self.value = KafkaMessage(data: data, key: key)
		self.size = nil
    }

    init(data: inout Data) {
        offset = Int64(data: &data)
        size = Int32(data: &data)
        value = KafkaMessage(data: &data)
    }
    
    var messageSizeData: Data {
        return (Int32(self.value.dataLength).data)
    }
    
    let messageSizeDataLength = 4
    
    var dataLength: Int {
        return self.offset.dataLength +
            self.messageSizeDataLength +
            self.value.dataLength +
            (self.size?.dataLength ?? 0)
    }
    
    var data: Data {
        var data = Data(capacity: self.dataLength)
        
        data.append(self.offset.data)
        data.append(self.messageSizeData)
        data.append(self.value.data)
        return data
    }
}

class KafkaMessage: KafkaType {
	///Message key
    let key: Data?
	///Message data
    let value: Data
    private var _magicByte: Int8
    private var _attributes: Int8
    private var _crc: UInt32! = nil

    /**
        Initialize a new message using raw bytes

        - Parameter data:   an NSData object
        - Parameter key:    an optional key String. Can be used for partition assignment.
     */
    init(data: Data, key: Data? = nil) {
        self._attributes = Int8(CompressionCodec.none.rawValue)
        self._magicByte = Int8(0)
        self.key = key
        self.value = data
    }
    
    /**
        Initialize a new message from a string

        - Parameter value:  String value
        - Parameter key:    an optional key String. Can be used for partition assignment.
     */
    init(value: String, key: String? = nil) {
        self._attributes = Int8(CompressionCodec.none.rawValue)
        self._magicByte = Int8(0)
        self.key = key?.data(using: .utf8)
		self.value = value.data(using: .utf8)!
    }

    /**
        Initialize a new message from raw bytes received from a pull request

        - Parameter value:  String value
        - Parameter key:    an optional key String. Can be used for partition assignment.
     */
    required init(data: inout Data) {
        _crc = UInt32(data: &data)
        _magicByte = Int8(data: &data)
        _attributes = Int8(data: &data)
        key = Data(data: &data)
        value = Data(data: &data)
    }
    
    lazy var dataLength: Int = {
        return self.valueLength + 4
    }()
    
    lazy var valueLength: Int = {
        return _magicByte.dataLength + _attributes.dataLength + key.dataLength + value.dataLength
    }()
    
    lazy var data: Data = {
        var valueData = Data(capacity: self.valueLength)
        valueData.append(self._magicByte.data)
        valueData.append(self._attributes.data)
        valueData.append(key.data)
        valueData.append(value.data)
        
		self._crc = CRC32(data: valueData).crc
        
        var data = Data(capacity: self.dataLength)
        data.append(self._crc.data)
        data.append(valueData)
        return data
    }()
}


/**
    A Message pulled from the Kafka Server
*/
//TODO: Convert to struct
@objc public class Message: NSObject {
	
    /**
        Message data
    */
    public let value: Data

    /**
        Message key
     */
    public let key: Data?

    /**
        Initialize a new message using raw bytes
     
        - Parameter data:   an NSData object
        - Parameter key:    an optional key String. Can be used for partition assignment.
    */
    internal init(data: Data, key: Data? = nil) {
        self.key = key
        self.value = data
    }
}
