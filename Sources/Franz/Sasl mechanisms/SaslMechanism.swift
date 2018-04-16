//
//  SaslMechanism.swift
//  FranzTests
//
//  Created by Luke Lau on 15/04/2018.
//

import Foundation

protocol SaslMechanism {
	
	/// Does the authentication mechanism with the server
	/// Returns true if successful, false otherwise
	func authenticate(connection: Connection) -> Bool
	var kafkaLabel: String { get }
}
