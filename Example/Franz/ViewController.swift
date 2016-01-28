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

    override func viewDidLoad() {
        super.viewDidLoad()
        //"52.11.231.196": 9092,
        //"52.35.76.242": 9092,
        //"52.10.67.78": 9092,

        let cluster = Cluster(
            brokers: [
                ("127.0.0.1", 9092),
                ("127.0.0.1", 9093),
                ("127.0.0.1", 9094),
                ("127.0.0.1", 9095)
            ],
            clientId: "local-test"
            //nodeId: 1
        )

        cluster.joinGroup("my-group", topics:["replica"])
        /*
        //cluster.sendMessage("example", partition: 0, message: "uhhh.")
        cluster.consumeMessages("replica", partition: 0) { messages in
            for message in messages {
                print(message.value)
            }
        }

        cluster.consumeMessages("replica", partition: 1) { messages in
            for message in messages {
                print(message.value)
            }
        }

        cluster.listGroups { group in
            print("Group Found: \(group.id)")
            //cluster.joinGroup("example-group", topics: ["example"])
        }
        
        cluster.listTopics { topics in
            for topic in topics {
                //print("TOPIC FOUND \(topic.name)")
                cluster.getOffsets(topic.name, partition: 0) { offsets in
                    //print("\tOFFSETS: \(offsets)")
                    if offsets.count == 2 {
                        cluster.getMessage(topic.name, partition: 0, offset: offsets[1] - 1) { message in
                            //print("\t\tMESSAGE: \(message.value)")
                        }
                    }
                }
            }
        }
        */
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }

}

