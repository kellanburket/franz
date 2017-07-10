//
//  MessageSet.swift
//  Franz
//
//  Created by Kellan Cummings on 1/20/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


class MessageSet: KafkaClass {
    var _values: [MessageSetItem]
    
    var messages: [Message] {
        var messages = [Message]()

        for value in _values {
            messages.append(value.message)
        }
        
        return messages
    }
    
    init(values: [MessageSetItem]) {
        self._values = values
    }

    required init(bytes: inout [UInt8]) {
        _values = [MessageSetItem]()
        
        while bytes.count > 0 {
            _values.append(MessageSetItem(bytes: &bytes))
        }
    }
    
    lazy var length: Int = {
        return self.valueLength + 4
    }()
    
    lazy var valueLength: Int = {
        var totalLength = 0
        
        for value in self._values {
            totalLength += value.length
        }
        
        return totalLength
    }()
    
    lazy var valueLengthData: Data = {
        return (Int32(self.valueLength).data)
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        
        data.append(self.valueLengthData)
        
        for value in self._values {
            print("Appending \(value)")
            data.append(value.data)
        }
        
        return data
    }()
    
    var description: String {
        var str = ""
        
        for value in _values {
            str += value.description
        }
        return str
    }
}


class MessageSetItem: KafkaClass {
    var _offset: KafkaInt64
    var _value: KafkaMessage
    var _size: KafkaInt32?
    
    var offset: Int64 {
        return _offset.value
    }
    
    var message: Message {
        return Message(data: _value.value, key: _value.key)
    }
    
    init(value: String, key: String? = nil, offset: Int = 0) {
        self._offset = KafkaInt64(value: Int64(offset))
        self._value = KafkaMessage(value: value, key: key)
    }

    init(data: Data, key: Data? = nil, offset: Int = 0) {
        self._offset = KafkaInt64(value: Int64(offset))
        self._value = KafkaMessage(data: data, key: key)
    }

    required init(bytes: inout [UInt8]) {
        _offset = KafkaInt64(bytes: &bytes)
        _size = KafkaInt32(bytes: &bytes)
        _value = KafkaMessage(bytes: &bytes)
    }
    
    lazy var messageSizeData: Data = {
        return (Int32(self._value.length).data)
    }()
    
    let messageSizeDataLength = 4
    
    lazy var length: Int = {
        return self._offset.length +
            self.messageSizeDataLength +
            self._value.length +
            (self._size != nil ? self._size!.length : 0)
    }()
    
    lazy var data: Data = {
        var data = Data(capacity: self.length)
        
        data.append(self._offset.data)
        data.append(self.messageSizeData)
        data.append(self._value.data)
        return data
    }()
    
    var description: String {
        return "\n\t\t\t\t\t---------\n" +
            "\t\t\t\t\tLENGTH: (\(length))\n" +
            "\t\t\t\t\tOFFSET: \(_offset.data)\n" +
            "\t\t\t\t\tMESSAGE SIZE: \(messageSizeData)\n" +
            "\t\t\t\t\tMESSAGE: \n\(_value.description)\n"
    }
}

class KafkaMessage: KafkaClass {
    var _key: KafkaBytes
    var _value: KafkaBytes
    var _magicByte: KafkaInt8
    var _attributes: KafkaInt8
    var _crc: KafkaUInt32! = nil
    
    /**
        Message data
    */
    var value: Data {
        return _value.valueData
    }

    /**
        Message key
     */
    var key: Data? {
        return _key.valueData
    }

    /**
        Initialize a new message using raw bytes

        - Parameter data:   an NSData object
        - Parameter key:    an optional key String. Can be used for partition assignment.
     */
    init(data: Data, key: Data? = nil) {
        self._attributes = KafkaInt8(value: CompressionCodec.none.rawValue)
        self._magicByte = KafkaInt8(value: 0)
        self._key = KafkaBytes(value: key)
        self._value = KafkaBytes(value: data)
    }
    
    /**
        Initialize a new message from a string

        - Parameter value:  String value
        - Parameter key:    an optional key String. Can be used for partition assignment.
     */
    init(value: String, key: String? = nil) {
        self._attributes = KafkaInt8(value: CompressionCodec.none.rawValue)
        self._magicByte = KafkaInt8(value: 0)
        self._key = KafkaBytes(value: key)
        self._value = KafkaBytes(value: value)
    }

    /**
        Initialize a new message from raw bytes received from a pull request

        - Parameter value:  String value
        - Parameter key:    an optional key String. Can be used for partition assignment.
     */
    required init(bytes: inout [UInt8]) {
        _crc = KafkaUInt32(bytes: &bytes)
        _magicByte = KafkaInt8(bytes: &bytes)
        _attributes = KafkaInt8(bytes: &bytes)
        _key = KafkaBytes(bytes: &bytes)
        _value = KafkaBytes(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self.valueLength + 4
    }()
    
    lazy var valueLength: Int = {
        return self._magicByte.length +
            self._attributes.length +
            self._key.length +
            self._value.length
    }()
    
    lazy var data: Data = {
        var valueData = Data(capacity: self.valueLength)
        valueData.append(self._magicByte.data)
        valueData.append(self._attributes.data)
        valueData.append(self._key.data)
        valueData.append(self._value.data)
        
		self._crc = KafkaUInt32(value: CRC32(data: valueData).crc)
        
        var data = Data(capacity: self.length)
        data.append(self._crc.data)
        data.append(valueData)
        return data
    }()
    
    var description: String {
        return "\t\t\t\t\t\tMAGIC_BYTE(\(self._crc.length)): \(self._crc.value) => \(self._crc.data)\n" +
            "\t\t\t\t\t\tMAGIC_BYTE(\(self._magicByte.length)): \(self._magicByte.value) => \(self._magicByte.data)\n" +
            "\t\t\t\t\t\tATTRIBUTES(\(self._attributes.length)): \(self._attributes.value) => \(self._attributes.data)\n" +
            "\t\t\t\t\t\tKEY(\(self._key.length)): \(self._key.value?.description ?? "nil") => \(self._key.data)\n" +
		"\t\t\t\t\t\tVALUE(\(self._value.length)): \(self._value.value?.description ?? "nil") => \(self._value.data)\n"
    }
}


/**
    A Message pulled from the Kafka Server
*/
open class Message: NSObject {
    var _key: Data?
    var _value: Data
    
    /**
        Message data
    */
    open var value: Data {
		return _value 
    }

    /**
        Message key
     */
    open var key: Data? {
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
