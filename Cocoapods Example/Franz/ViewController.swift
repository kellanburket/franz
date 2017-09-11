//
//  ViewController.swift
//  Franz
//
//  Created by kellanburket on 01/24/2016.
//  Copyright (c) 2016 kellanburket. All rights reserved.
//

import UIKit
import Franz

class ViewController: UIViewController {

    var cluster: Cluster!
    
    override func viewDidLoad() {
        super.viewDidLoad()
        
        cluster = Cluster(
            brokers: [
                ("localhost", 9092)
            ],
            clientId: "replica-test"
        )
        
        let consumer = cluster.getConsumer(topics: ["test"], groupId: "newGroup")
		consumer.listen { message in
			print(String(data: message.value, encoding: .utf8)!)
		}
		
		var count = 0
		
		Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { timer in
			self.cluster.sendMessage("test", message: "\(count)")
			count += 1
		}
    }
	
}

