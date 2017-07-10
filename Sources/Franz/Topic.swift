//
//  Topic.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


open class Topic: NSObject {
    var _name: String
    var _partitions: [Int32]
    
    open var name: String {
        return _name
    }
    
    open var partitions: [Int32] {
        return _partitions
    }
    
    internal init(name: String, partitions: [Int32]) {
        self._name = name
        self._partitions = partitions
    }
}

internal class KafkaTopic: KafkaClass {
    var _errorCode: KafkaInt16
    var _topicName: KafkaString
    var _partitionMetadata: KafkaArray<Partition>
    
    var error: KafkaErrorCode? {
        if let error = KafkaErrorCode(rawValue: _errorCode.value) {
            return error
        } else {
            return nil
        }
    }
    
    var partitions: [Int32: Partition] {
        
        var values = [Int32: Partition]()
        for value in _partitionMetadata.values {
            values[value.id] = value
        }
        
        return values
    }
    
    var name: String? {
        return _topicName.value
    }
    
    var description: String {
		var description = """
			TOPIC METADATA
			ERROR CODE: \(error?.code ?? 0)
			ERROR DESCRIPTION: \(error?.description ?? "nil")
			TOPIC: \(name ?? "nil")
		"""
		
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
    
    required init(bytes: inout [UInt8]) {
        _errorCode = KafkaInt16(bytes: &bytes)
        _topicName = KafkaString(bytes: &bytes)
        _partitionMetadata = KafkaArray(bytes: &bytes)
    }
    
    lazy var length: Int = {
        return self._errorCode.length
    }()
    
    var data: Data {
        return Data()
    }
}
