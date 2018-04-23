//
//  Topic.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

public typealias TopicName = String

public class Topic: NSObject {
    private var _name: TopicName
    private var _partitions: [Int32]
    
    public var name: String {
        return _name
    }
    
    public var partitions: [Int32] {
        return _partitions
    }
    
    internal init(name: String, partitions: [Int32]) {
        self._name = name
        self._partitions = partitions
    }
}

internal class KafkaTopic: KafkaType {
    private var _errorCode: Int16
    private var _topicName: String
    private var _partitionMetadata: [Partition]
    
    var error: KafkaErrorCode? {
        if let error = KafkaErrorCode(rawValue: _errorCode) {
            return error
        } else {
            return nil
        }
    }
    
    var partitions: [Int32: Partition] {
        
        var values = [Int32: Partition]()
        for value in _partitionMetadata {
            values[value.id] = value
        }
        
        return values
    }
    
    var name: String? {
        return _topicName
    }
    
    init(errorCode: Int, name: String, partitionMetadata: [Partition]) {
        self._errorCode = Int16(errorCode)
        self._topicName = name
        self._partitionMetadata = partitionMetadata
    }
    
    required init(data: inout Data) {
        _errorCode = Int16(data: &data)
        _topicName = String(data: &data)
        _partitionMetadata = [Partition](data: &data)
    }
    
    lazy var dataLength: Int = {
        return self._errorCode.dataLength
    }()
    
    var data: Data {
        return _errorCode.data + _topicName.data + _partitionMetadata.data
    }
}
