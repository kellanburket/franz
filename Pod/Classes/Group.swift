//
//  Group.swift
//  Pods
//
//  Created by Kellan Cummings on 1/26/16.
//
//

import Foundation

public class Group {
    private var _broker: Broker
    private var _id: String
    private var _type: String
    
    public var id: String {
        return _id
    }
    
    public var type: String {
        return _type
    }

    internal init(id: String, type: String, broker: Broker) {
        self._broker = broker
        self._type = type
        self._id = id
    }
}