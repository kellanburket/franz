//
//  Partition.swift
//  Franz
//
//  Created by Kellan Cummings on 1/22/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

typealias PartitionId = Int32

class Partition: KafkaType {
	
    private var partitionErrorCode: Int16
    var error: KafkaErrorCode? {
        return KafkaErrorCode(rawValue: partitionErrorCode)
    }
    
    var id: PartitionId
    
    var leader: PartitionId
    
    private(set) var replicas: [Int32]
    
    private(set) var isr: [Int32]
    
    init(partitionErrorCode: Int, partitionId: Int, leader: Int, replicas: [Int], isr: [Int]) {
        self.partitionErrorCode = Int16(partitionErrorCode)
        self.id = Int32(partitionId)
        self.leader = Int32(leader)
		
		self.replicas = [Int32](replicas.map { Int32($0) })
		self.isr = [Int32](isr.map { Int32($0) })
    }
    
	required init(data: inout Data) {
        partitionErrorCode = Int16(data: &data)
        id = PartitionId(data: &data)
        leader = PartitionId(data: &data)
        replicas = [Int32](data: &data)
        isr = [Int32](data: &data)
    }
    
    var dataLength: Int {
        return partitionErrorCode.dataLength + id.dataLength + leader.dataLength + replicas.dataLength + isr.dataLength
    }
    
    var data: Data {
        return partitionErrorCode.data + id.data + leader.data + replicas.data + isr.data
    }
}
