//
//  Constants.swift
//  Franz
//
//  Created by Kellan Cummings on 1/23/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

var MainQueue: DispatchQueue {
    return DispatchQueue.main
}

var UserInteractiveQueue: DispatchQueue {
    return DispatchQueue.global(qos: DispatchQoS.QoSClass.userInteractive)
}

var UserInitiatedQueue: DispatchQueue {
    return DispatchQueue.global(qos: DispatchQoS.QoSClass.userInitiated)
}

var UtilityQueue: DispatchQueue {
    return DispatchQueue.global(qos: DispatchQoS.QoSClass.utility)
}

var BackgroundQueue: DispatchQueue {
    return DispatchQueue.global(qos: DispatchQoS.QoSClass.background)
}
