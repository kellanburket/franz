//
//  KafkaDecoder.swift
//  Franz
//
//  Created by Luke Lau on 24/04/2018.
//

import Foundation

public class KafkaDecoder: Decoder {
	
	enum DecodingError: Error {
		case blah
	}
	
	public let codingPath: [CodingKey] = []
	
	public let userInfo: [CodingUserInfoKey : Any] = [:]
	
	public static func decode<T: Decodable>(_ type: T.Type, data: Data) throws -> T {
		let decoder = KafkaDecoder(data: data)
		return try decoder.decode(T.self)
	}
	
	fileprivate var storage: Data
	private init(data: Data) {
		self.storage = data
	}
	
	fileprivate func decode<T: Decodable>(_ type: T.Type) throws -> T {
		print("decoding \(type)")
		return try type.init(from: self)
	}
	
	private struct KeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
		
		let decoder: KafkaDecoder
		let codingPath: [CodingKey] = []
		let allKeys: [Key] = []
		
		func contains(_ key: Key) -> Bool {
			return true
		}
		
		func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
			switch type {
			case let v as KafkaDecodable.Type:
				return try v.decode(with: decoder) as! T
			default:
				return try decoder.decode(type)
			}
		}
		
		func decodeNil(forKey key: Key) throws -> Bool {
			return true
		}
		
		func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
			return try decoder.container(keyedBy: type)
		}
		
		func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
			return try decoder.unkeyedContainer()
		}
		
		func superDecoder() throws -> Decoder {
			return decoder
		}
		
		func superDecoder(forKey key: Key) throws -> Decoder {
			return decoder
		}
	}
	
	private struct SingleValueContainer: SingleValueDecodingContainer {
		let decoder: KafkaDecoder
		let codingPath: [CodingKey] = []
		
		func decodeNil() -> Bool { return true }
		
		func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
			switch type {
			case let v as KafkaDecodable.Type:
				return try v.decode(with: decoder) as! T
			default:
				return try decoder.decode(type)
			}
		}
	}
	
	private struct UnkeyedContainer: UnkeyedDecodingContainer {
		let decoder: KafkaDecoder
		let codingPath: [CodingKey] = []
		let count: Int?
		private(set) var currentIndex = 0
		private(set) var isAtEnd: Bool
		
		init(decoder: KafkaDecoder) throws {
			self.decoder = decoder
			self.count = Int(try decoder.decode(Int32.self))
			self.currentIndex = 0
			print("Started decoding an array of size \(count)")
			isAtEnd = count! <= 0
		}
		
		mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
			currentIndex += 1
			if currentIndex >= count! {
				isAtEnd = true
			}
			print("decoding \(type) in an unkeyed array at index \(currentIndex)")
			switch type {
			case let v as KafkaDecodable.Type:
				return try v.decode(with: decoder) as! T
			default:
				return try decoder.decode(type)
			}
		}
		
		func decodeIfPresent(_ type: String.Type) throws -> String? {
			let length = Int(try decoder.decode(Int16.self))
			if length == -1 {
				return nil
			}
			return String(data: decoder.storage.take(first: length), encoding: .utf8)!
		}
		
		func decodeNil() -> Bool {
			return true
		}
		
		func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
			return try decoder.container(keyedBy: type)
		}
		
		func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
			return try UnkeyedContainer(decoder: decoder)
		}
		
		func superDecoder() throws -> Decoder {
			return decoder
		}
	}
	
	public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
		return KeyedDecodingContainer(KeyedContainer<Key>(decoder: self))
	}
	
	public func unkeyedContainer() throws -> UnkeyedDecodingContainer {
		return try UnkeyedContainer(decoder: self)
	}
	
	public func singleValueContainer() throws -> SingleValueDecodingContainer {
		return SingleValueContainer(decoder: self)
	}
}

private protocol KafkaDecodable: Decodable {
	static func decode(with decoder: KafkaDecoder) throws -> Self
}

extension KafkaDecodable where Self: FixedWidthInteger {
	static func decode(with decoder: KafkaDecoder) throws -> Self {
		let dataLength = MemoryLayout<Self>.size
		print("decoding an integer in a single value container: \(Self.self)")
		print("\(decoder.storage.count) bytes left")
		let res = Self.init(bigEndian: decoder.storage.take(first: dataLength).withUnsafeBytes { $0.pointee })
		print("got \(res)")
		return res
	}
}

extension String: KafkaDecodable {
	static func decode(with decoder: KafkaDecoder) throws -> String {
		let length = Int(try decoder.decode(Int16.self))
		if length == -1 {
			fatalError("Unexpected nil string")
		}
		return String(data: decoder.storage.take(first: length), encoding: .utf8)!
	}
}


extension Bool: KafkaDecodable {
	static func decode(with decoder: KafkaDecoder) throws -> Bool {
		return try decoder.decode(Int8.self) == 1
	}
}

extension Data: KafkaDecodable {
	static func decode(with decoder: KafkaDecoder) throws -> Data {
		let length = Int(try decoder.decode(Int32.self))
		if length == -1 {
			//TODO: handle optionals
			fatalError("Need to handle optionals")
		}
		return decoder.storage.take(first: length)
	}
}


extension UInt: KafkaDecodable {}
extension UInt8: KafkaDecodable {}
extension UInt16: KafkaDecodable {}
extension UInt32: KafkaDecodable {}
extension UInt64: KafkaDecodable {}
extension Int: KafkaDecodable {}
extension Int8: KafkaDecodable {}
extension Int16: KafkaDecodable {}
extension Int32: KafkaDecodable {}
extension Int64: KafkaDecodable {}

extension Data {
	mutating func take(first: Int) -> Data {
		let start = prefix(upTo: startIndex + first)
		removeFirst(first)
		return start
	}
}
