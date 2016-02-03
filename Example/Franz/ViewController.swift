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
                ("127.0.0.1", 9092)
                //("localhost", 9093),
                //("localhost", 9094),
                //("localhost", 9095)
            ],
            clientId: "replica-test"
        )
        
        /*

        //cluster.sendMessage("replica", partition: 0, message: "uhhh.")
       */
        cluster.joinGroup("replica-test", topics:["replica"]) { membership in
            print("JOINING GROUP: \(membership.group.id)")
            
            cluster.listGroups { groupId, groupProtocolType in
                print("LISTING GROUPS: \(groupId) => \(groupProtocolType)")
            }
            
            membership.group.getState { state in
                membership.sync(["replica": [0]]) {
                    print("SYNC COMPLETE")
                }
                /*
                cluster.consumeMessages("replica", partition: 0, groupId: groupId) { message in
                    print("CONSUMING MESSAGE: \(message)")
                }
                */
            }
        }

        /*
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

