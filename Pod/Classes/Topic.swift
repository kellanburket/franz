//
//  Topic.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


public class Topic: KafkaClass {
    private var _errorCode: KafkaInt16
    private var _topicName: KafkaString
    private var _partitionMetadata: KafkaArray<Partition>
    
    var error: KafkaErrorCode? {
        if let error = KafkaErrorCode(rawValue: _errorCode.value) {
            return error
        } else {
            return nil
        }
    }
    
    public var partitions: [Int32: Partition] {
        
        var values = [Int32: Partition]()
        for value in _partitionMetadata.values {
            values[value.id] = value
        }
        
        return values
    }
    
    public var name: String? {
        return _topicName.value
    }
    
    var description: String {
        let defaultValue = "nil"
        var description = "TOPIC METADATA\n\t" +
            "ERROR CODE: \(error?.code ?? 0)\n\t" +
            "ERROR DESCRIPTION: \(error?.description ?? defaultValue)\n\t" +
        "TOPIC: \(name)\n"
        
        for (_, partition) in partitions {
            description += "----------\n\(partition.description)\n"
        }
        
        return description
    }
    
    init(errorCode: Int, name: String, partitionMetadata: [Partition]) {
        self._errorCode = KafkaInt16(value: Int16(errorCode))
        self._topicName = KafkaString(value: name)
        self._partitionMetadata = KafkaArray(values: partitionMetadata)
    }
    
    required public init(inout bytes: [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _topicName = KafkaString(bytes: &bytes)
        _partitionMetadata = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length
    }()
    
    var data: NSData {
        return NSData()
    }
    
}
