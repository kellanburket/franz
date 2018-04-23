//
//  MessageSet.swift
//  Franz
//
//  Created by Kellan Cummings on 1/20/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


class MessageSet: KafkaType {
    let values: [MessageSetItem]
	
    init(values: [MessageSetItem]) {
        self.values = values
    }

    required init(data: inout Data) {
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


class MessageSetItem: KafkaType {
    var _offset: Int64
    var _value: KafkaMessage
    var _size: Int32?
    
    var offset: Int64 {
        return _offset
    }
    
    var message: Message {
        return Message(data: _value.value, key: _value.key)
    }
    
    init(value: String, key: String? = nil, offset: Int = 0) {
        self._offset = Int64(offset)
        self._value = KafkaMessage(value: value, key: key)
    }

    init(data: Data, key: Data? = nil, offset: Int = 0) {
        self._offset = Int64(offset)
        self._value = KafkaMessage(data: data, key: key)
    }

    required init(data: inout Data) {
        _offset = Int64(data: &data)
        _size = Int32(data: &data)
        _value = KafkaMessage(data: &data)
    }
    
    lazy var messageSizeData: Data = {
        return (Int32(self._value.dataLength).data)
    }()
    
    let messageSizeDataLength = 4
    
    lazy var dataLength: Int = {
        return self._offset.dataLength +
            self.messageSizeDataLength +
            self._value.dataLength +
            (self._size != nil ? self._size!.dataLength : 0)
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.dataLength)
        
        data.append(self._offset.data)
        data.append(self.messageSizeData)
        data.append(self._value.data)
        return data
    }()
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
public class Message: NSObject {
    private var _key: Data?
    private var _value: Data
    
    /**
        Message data
    */
    public var value: Data {
		return _value 
    }

    /**
        Message key
     */
    public var key: Data? {
        return _key
    }

    /**
        Initialize a new message using raw bytes
     
        - Parameter data:   an NSData object
        - Parameter key:    an optional key String. Can be used for partition assignment.
    */
    internal init(data: Data, key: Data? = nil) {
        self._key = key
        self._value = data
    }
}
