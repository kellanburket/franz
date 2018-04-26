//
//  KafkaEncoder.swift
//  Franz
//
//  Created by Luke Lau on 24/04/2018.
//

import Foundation

public class KafkaEncoder: Encoder {
	
	public init() {}
	
	fileprivate(set) var storage = Data()
	
	private struct KeyedContainer<Key: CodingKey>: KeyedEncodingContainerProtocol {
		var encoder: KafkaEncoder
		public let codingPath: [CodingKey] = []
		
		func encode<T>(_ value: T, forKey key: Key) throws where T : Encodable {
			switch value {
			case let v as KafkaEncodable:
				try v.encode(to: encoder)
			default:
				try value.encode(to: encoder)
			}
		}
		
		func encodeNil(forKey key: Key) throws {}
		
		func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
			return encoder.container(keyedBy: keyType)
		}
		
		func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
			return encoder.unkeyedContainer()
		}
		
		func superEncoder() -> Encoder {
			return encoder
		}
		
		func superEncoder(forKey key: Key) -> Encoder {
			return encoder
		}
	}
	
	private struct UnkeyedContanier: UnkeyedEncodingContainer {
		let encoder: KafkaEncoder
		
		let codingPath: [CodingKey] = []
		
		private(set) var count = 0
		
		private let countRange: CountableRange<Int>
		
		init(encoder: KafkaEncoder) {
			self.encoder = encoder
			let startCountIndex = encoder.storage.count
			countRange = startCountIndex..<(startCountIndex + MemoryLayout<Int32>.size)
			
			let subEncoder = KafkaEncoder()
			try! Int32(count).encode(to: subEncoder)
			encoder.storage += subEncoder.storage
		}
		
		func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey {
			return encoder.container(keyedBy: keyType)
		}
		
		func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
			return self
		}
		
		func superEncoder() -> Encoder {
			return encoder
		}
		
		func encodeNil() throws {}
		
		//		func encode<T>(contentsOf sequence: T) throws where T : Sequence {
		//			//TODO: check this out for array/string/data?
		//		}
		
		mutating private func incrementCount() throws {
			count += 1
			let subEncoder = KafkaEncoder()
			try Int32(count).encode(to: subEncoder)
			encoder.storage[countRange] = subEncoder.storage
		}
		
		mutating func encode<T>(_ value: T) throws where T: Encodable {
			try incrementCount()
			print("encoding type \(type(of: value)) into an unkeyed container")
			try value.encode(to: encoder)
		}
	}
	
	private struct SingleValueContainer: SingleValueEncodingContainer {
		let codingPath: [CodingKey] = []
		let encoder: KafkaEncoder
		
		func encodeNil() throws {}
		
		func encode<T>(_ value: T) throws where T : Encodable {
			switch value {
			case let v as KafkaEncodable:
				try v.encode(to: encoder)
			default:
				try value.encode(to: encoder)
			}
		}
	}
	
	public var codingPath: [CodingKey] { return [] }
	public var userInfo: [CodingUserInfoKey : Any] { return [:] }
	
	public func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key : CodingKey {
		return KeyedEncodingContainer(KeyedContainer<Key>(encoder: self))
	}
	
	public func unkeyedContainer() -> UnkeyedEncodingContainer {
		return UnkeyedContanier(encoder: self)
	}
	
	public func singleValueContainer() -> SingleValueEncodingContainer {
		return SingleValueContainer(encoder: self)
	}
	
}

private protocol KafkaEncodable: Encodable {
	func encode(to encoder: KafkaEncoder) throws
}

extension KafkaEncodable where Self: FixedWidthInteger {
	func encode(to encoder: KafkaEncoder) {
		var bytes = Self(self.bigEndian)
		encoder.storage += Data(bytes: &bytes, count: MemoryLayout<Self>.size)
	}
}

extension String: KafkaEncodable {
	func encode(to encoder: KafkaEncoder) throws {
		guard let stringData = data(using: .utf8) else {
			//TODO: Implement coding paths
			let context = EncodingError.Context(codingPath: [],
												debugDescription: "Failed to convert String to UTF-8 data")
			throw EncodingError.invalidValue(self, context)
		}
		try Int16(count).encode(to: encoder)
		encoder.storage += stringData
	}
}

extension Data: KafkaEncodable {
	func encode(to encoder: KafkaEncoder) throws {
		try Int32(count).encode(to: encoder)
		encoder.storage += self
	}
}

extension Bool: KafkaEncodable {
	func encode(to encoder: KafkaEncoder) throws {
		try Int8(self ? 1 : 0).encode(to: encoder)
	}
}

extension UInt: KafkaEncodable {}
extension UInt8: KafkaEncodable {}
extension UInt16: KafkaEncodable {}
extension UInt32: KafkaEncodable {}
extension UInt64: KafkaEncodable {}
extension Int: KafkaEncodable {}
extension Int8: KafkaEncodable {}
extension Int16: KafkaEncodable {}
extension Int32: KafkaEncodable {}
extension Int64: KafkaEncodable {}
