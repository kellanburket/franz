//
//  SaslMechanism.swift
//  FranzTests
//
//  Created by Luke Lau on 15/04/2018.
//

import Foundation

/// A mechanism that SASL can use to authenticate.
protocol SaslMechanism {
	
	/// Does the authentication mechanism with the server
	/// Returns true if successful, false otherwise
	func authenticate(connection: Connection) -> Bool
	
	/// The name Kafka represents the mechanism with in the API.
	var kafkaLabel: String { get }
}
