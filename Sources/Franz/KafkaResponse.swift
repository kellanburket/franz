//
//  KafkaResponse.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

protocol KafkaResponse: Readable {    
    
    init(bytes: inout [UInt8])
	
	var description: String { get }

}
