//
//  KafkaProtocol.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

protocol KafkaType {
	init(data: inout Data)
	var data: Data { get }
	var dataLength: Int { get }
}

protocol CompoundKafkaType: KafkaType {
	var values: [KafkaType] { get }
}

extension CompoundKafkaType {
	var data: Data {
		return values.map { $0.data }.reduce(Data(), +)
	}
	
	var dataLength: Int {
		return values.map { $0.dataLength }.reduce(0, +)
	}
}

extension Data {
	mutating func take(first: Int) -> Data {
		let start = prefix(upTo: startIndex + first)
		removeFirst(first)
		return start
	}
}

extension KafkaType where Self: FixedWidthInteger {
	init(data: inout Data) {
		let dataLength = MemoryLayout<Self>.size
		self.init(bigEndian: data.take(first: dataLength).withUnsafeBytes { $0.pointee })
	}
	
	var dataLength: Int {
		return MemoryLayout<Self>.size
	}
	
	var data: Data {
		var bytes = Self(self.bigEndian)
		return Data(bytes: &bytes, count: MemoryLayout<Self>.size)
	}
}

extension UInt: KafkaType {}
extension UInt8: KafkaType {}
extension UInt16: KafkaType {}
extension UInt32: KafkaType {}
extension UInt64: KafkaType {}
extension Int: KafkaType {}
extension Int8: KafkaType {}
extension Int16: KafkaType {}
extension Int32: KafkaType {}
extension Int64: KafkaType {}

extension Bool: KafkaType {
	init(data: inout Data) {
		self = Int8(data: &data) == 0
	}
	
	private var representation: Int8 {
		return Int8(self ? 1 : 0)
	}
	
	var data: Data {
		return representation.data
	}
	
	var dataLength: Int {
		return representation.dataLength
	}
}

protocol KafkaVariableLengthType: KafkaType {
	associatedtype Length: FixedWidthInteger where Length: KafkaType
	
	var variableLengthData: Data { get }
	init(variableLengthData: Data)
}

extension KafkaVariableLengthType {
	
	init(data: inout Data) {
		let length = Int(Length(data: &data))
		if length == -1 {
			self.init(variableLengthData: Data())
			return
		}
		self.init(variableLengthData: data.take(first: length))
	}
	
	var lengthSize: Int {
		return MemoryLayout<Length>.size
	}
	
	var dataLength: Int {
		return variableLengthData.count + lengthSize
	}
	
	var data: Data {
		var finalData = Data(capacity: self.dataLength)
		finalData += Length(self.variableLengthData.count).data
		finalData += variableLengthData
		return finalData
	}
	
}

extension String: KafkaVariableLengthType {
	typealias Length = Int16
	
	init(variableLengthData: Data) {
		self.init(data: variableLengthData, encoding: .utf8)!
	}
	
	var variableLengthData: Data {
		return data(using: .utf8)!
	}
}

extension Optional where Wrapped == String {
	var dataLength: Int {
		switch self {
		case .none:
			return "".dataLength
		case .some(let wrapped):
			return wrapped.dataLength
		}
	}
	
	var data: Data {
		switch self {
		case .none:
			return "".data
		case .some(let wrapped):
			return wrapped.data
		}
	}
}

extension Data: KafkaVariableLengthType {
	typealias Length = Int32
	
	init(variableLengthData: Data) {
		self = variableLengthData
	}
	
	var variableLengthData: Data {
		return self
	}
}

extension Optional where Wrapped == Data {
	var dataLength: Int {
		switch self {
		case .none:
			return Data().dataLength
		case .some(let wrapped):
			return wrapped.dataLength
		}
	}
	
	var data: Data {
		switch self {
		case .none:
			return Data().data
		case .some(let wrapped):
			return wrapped.data
		}
	}
}

extension Array: KafkaType where Element: KafkaType {
	init(data: inout Data) {
		let count = Int32(data: &data)
		
		var temp: [Element] = []
		for _ in 0..<count {
			temp.append(Element(data: &data))
		}
		self = temp
	}
	
	var data: Data {
		var finalData = Data(capacity: dataLength)
		
		let sizeData = Int32(count).data
		
		finalData.append(sizeData)
		finalData.append(valuesData)
		
		return finalData
	}
	
	var dataLength: Int {
		// Value length + size length
		return valuesDataLength + 4
	}
	
	private var valuesDataLength: Int {
		return map { $0.dataLength }.reduce(0, +)
	}
	
	private var valuesData: Data {
		return map { $0.data }.reduce(Data(), +)
	}
}

protocol KafkaMetadata: KafkaType {
    static var protocolType: GroupProtocol { get }
}
