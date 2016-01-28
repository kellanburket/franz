//
//  Constants.swift
//  Franz
//
//  Created by Kellan Cummings on 1/23/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

var MainQueue: dispatch_queue_t {
    return dispatch_get_main_queue()
}

var UserInteractiveQueue: dispatch_queue_t {
    return dispatch_get_global_queue(QOS_CLASS_USER_INTERACTIVE, 0)
}

var UserInitiatedQueue: dispatch_queue_t {
    return dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0)
}

var UtilityQueue: dispatch_queue_t {
    return dispatch_get_global_queue(QOS_CLASS_UTILITY, 0)
}

var BackgroundQueue: dispatch_queue_t {
    return dispatch_get_global_queue(QOS_CLASS_BACKGROUND, 0)
}