//
//  Partition.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

class Partition: KafkaClass {
    fileprivate var _partitionErrorCode: KafkaInt16
    fileprivate var _partitionId: KafkaInt32
    fileprivate var _leader: KafkaInt32
    fileprivate var _replicas: KafkaArray<KafkaInt32>
    fileprivate var _isr: KafkaArray<KafkaInt32>

    var error: KafkaErrorCode? {
        if let error = KafkaErrorCode(rawValue: _partitionErrorCode.value) {
            return error
        } else {
            return nil
        }
    }
    
    var id: Int32 {
        return _partitionId.value
    }
    
    var leader: Int32 {
        return _leader.value
    }
    
    var replicas: [Int32] {
        var values = [Int32]()
        for replica in _replicas.values {
            values.append(replica.value)
        }
        return values
    }
    
    var isr: [Int32] {
        var values = [Int32]()
        for isr in _isr.values {
            values.append(isr.value)
        }
        return values
    }
    
    lazy var description: String = {
        let defaultErrorValue = "nil"
        return "PARTITION METADATA\n\t" +
            "ERROR: \(self.error?.description ?? defaultErrorValue)\n\t" +
            "ERROR CODE: \(self.error?.code ?? 0)\n\t" +
            "PARTITION ID(\(self._partitionId.length)): \(self.id) => \(self._partitionId.data)\n\t" +
            "LEADER(\(self._leader.length)): \(self.leader) => \(self._leader.data)\n\t" +
            "REPLICAS(\(self._replicas.length)): \(self.replicas) => \(self._replicas.data)\n\t" +
        "ISR(\(self._isr.length)): \(self.isr) => \(self._isr.data)"
    }()
    
    init(
        partitionErrorCode: Int,
        partitionId: Int,
        leader: Int,
        replicas: [Int],
        isr: [Int]
    ) {
        _partitionErrorCode = KafkaInt16(value: Int16(partitionErrorCode))
        _partitionId = KafkaInt32(value: Int32(partitionId))
        _leader = KafkaInt32(value: Int32(leader))
        
        var tempReplicas = [KafkaInt32]()
        for replica in replicas {
            tempReplicas.append(KafkaInt32(value: Int32(replica)))
        }
        _replicas = KafkaArray<KafkaInt32>(values: tempReplicas)
        
        var tempIsr = [KafkaInt32]()
        for value in isr {
            tempIsr.append(KafkaInt32(value: Int32(value)))
        }
        _isr = KafkaArray<KafkaInt32>(values: tempIsr)
    }
    
    required init(bytes: inout [UInt8]) {
        _partitionErrorCode = KafkaInt16(bytes: &bytes)
        _partitionId = KafkaInt32(bytes: &bytes)
        _leader = KafkaInt32(bytes: &bytes)
        _replicas = KafkaArray(bytes: &bytes)
        _isr = KafkaArray(bytes: &bytes)
    }
    
    var length: Int {
        return _partitionErrorCode.length + _partitionId.length + _leader.length + _replicas.length + _isr.length
    }
    
    var data: Data {
        return Data()
    }
}
