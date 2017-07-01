//
//  KafkaProtocol.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


protocol Readable {
    init(bytes: inout [UInt8])
}


protocol KafkaType: Readable {
    var data: Data { get }
    var length: Int { get }
    var description: String { get }
}


class KafkaFixedLengthType<T: FixedLengthDatable>: KafkaType {
    var value: T
    
    required init(value: T) {
        self.value = value
    }
    
    required init(bytes: inout [UInt8]) {
		let slice = bytes.slice(0, length: MemoryLayout<T>.size)
        self.value = T(bytes: slice)
    }

    var length: Int {
        return MemoryLayout<T>.size
    }
    
    lazy var description: String = {
       return "(\(self.length)): \(self.value) => \(self.data)"
    }()
    
    lazy var data: Data = {
        return (self.value.data as Data)
    }()
}


class KafkaVariableLengthType<T: VariableLengthDatable, E: FixedLengthDatable>: KafkaType {
    var value: T?
    
    required init(value: T?) {
        self.value = value
    }

    required init(bytes: inout [UInt8]) {
		let sizeSlice = bytes.slice(0, length: MemoryLayout<E>.size)
        let size = E(bytes: sizeSlice).toInt()

        if size > 0 {
            let slice = bytes.slice(0, length: size)
            if let value = T.fromBytes(slice) as? T {
                self.value = value
            } else{
                self.value = T()
            }
        } else {
            self.value = T()
        }
    }

    lazy var length: Int = {
        if let value = self.value {
            return self.valueDataLength + self.sizeDataLength
        } else {
            return self.sizeDataLength
        }
    }()
    
    lazy var valueData: Data = {
        return self.value?.data ?? ((((((((NSData(data: ((((((E(-1).data as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data) as Data)
    }()

    lazy var valueDataLength: Int = {
        return self.valueData.count
    }()

    lazy var description: String = {
        return "(\(self.length)): \(self.value) => \(self.data)"
    }()

    lazy var data: Data = {
        let finalData = NSMutableData(capacity: self.length)!
        if let value = self.value {
            finalData.append(E(self.valueData.count).data as Data)
            finalData.append(self.valueData)
        } else {
            finalData.append(self.valueData)
        }
        return finalData as Data
    }()

    let sizeDataLength = MemoryLayout<E>.size
}


class KafkaArray<T: KafkaType>: KafkaType, Readable {
    
    var values: [T]
    
    required init(values: [T]) {
        self.values = values
    }
    
    required init(bytes: inout [UInt8]) {
        let sizeBytes = bytes.slice(0, length: 4)
        let count = Int32(bytes: sizeBytes)

        values = [T]()
        if count >= 0 {
            for _ in 0..<count {
                values.append(T(bytes: &bytes))
            }
        }
    }
    
    lazy var length: Int = {
        return self.valuesDataLength + self.sizeDataLength
    }()
    
    lazy var valuesDataLength: Int = {
        var totalLength = 0
        
        for value in self.values {
            totalLength += value.length
        }

        return totalLength
    }()

    lazy var valuesData: Data = {
        var valuesData = NSMutableData(
            capacity: self.valuesDataLength
        )!
        
        for value in self.values {
            valuesData.append(value.data)
        }
        
        return valuesData as Data
    }()

    let sizeDataLength = 4

    lazy var data: Data = {
        var finalData = NSMutableData(capacity: self.length)!

        let sizeData = Int32(self.values.count).data
        
        finalData.append(sizeData as Data)
        finalData.append(self.valuesData)

        return finalData as Data
    }()
    
    lazy var description: String = {
        var string = ""
        
        if self.values.count == 0 {
            return string
        }
        
        for value in self.values {
            string += "\(value.description), "
        }

        let endIndex = string.characters.index(string.startIndex, offsetBy: string.characters.count - 2)
        return string.substring(to: endIndex)
    }()
}


class KafkaInt8: KafkaFixedLengthType<Int8> {

    required init(value: Int8) {
        super.init(value: value)
    }
    
    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }

}


class KafkaInt16: KafkaFixedLengthType<Int16> {
    
    required init(value: Int16) {
        super.init(value: value)
    }

    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }

}


class KafkaInt32: KafkaFixedLengthType<Int32> {

    required init(value: Int32) {
        super.init(value: value)
    }

    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }

}


class KafkaUInt32: KafkaFixedLengthType<UInt32> {
    
    required init(value: UInt32) {
        super.init(value: value)
    }
    
    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }
    
}


class KafkaInt64: KafkaFixedLengthType<Int64> {
    
    required init(value: Int64) {
        super.init(value: value)
    }

    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }

}

class KafkaBytes: KafkaVariableLengthType<Data, Int32>, Hashable {
    
    var hashValue: Int {
        return value?.hashValue ?? -1
    }
    
    convenience init(value: String?) {
		self.init(value: value?.data)
    }
    
    required init(value: Data?) {
        super.init(value: value)
    }
    
    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }
	
}


class KafkaString: KafkaVariableLengthType<String, Int16>, Hashable {
    
    var hashValue: Int {
        return value?.hashValue ?? -1
    }
    
    required init(value: String?) {
        super.init(value: value)
    }

    required init(bytes: inout [UInt8]) {
        super.init(bytes: &bytes)
    }

}


func ==(lhs: KafkaString, rhs: KafkaString) -> Bool {
    return lhs.value == rhs.value
}

func ==(lhs: KafkaBytes, rhs: KafkaBytes) -> Bool {
    return lhs.value == rhs.value
}

protocol KafkaMetadata: KafkaType {
    static var protocolType: GroupProtocol { get }
}


protocol KafkaClass: KafkaType {}
