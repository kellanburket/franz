//
//  Connection.swift
//  Franz
//
//  Created by Kellan Cummings on 1/13/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

typealias RequestCallback = [UInt8] -> ()

enum ConnectionError: ErrorType {
    case UnableToOpenConnection
    case InvalidIpAddress
    case CannotProcessMessageData
    case InvalidCorrelationId
    case NoResponse
    case ZeroLengthResponse
    case PartialResponse(size: Int32)
    case InputStreamUnavailable
    case InputStreamError
    case UnableToFindInputStream
    case OutputStreamUnavailable
    case OutputStreamError(error: String)
    case OutputStreamHasEnded
    case OutputStreamClosed
    case UnableToWriteBytes
    case BytesNoLongerAvailable
}

class KafkaConnection: NSObject, NSStreamDelegate {
    
    private var ipv4: String

    var apiVersion: ApiVersion {
        return ApiVersion.DefaultVersion
    }

    private var _requestCallbacks = [Int32: RequestCallback]()
    private var _broker: Broker
    
    private var clientId: String
    
    private var readStream: Unmanaged<CFReadStream>?
    private var writeStream: Unmanaged<CFWriteStream>?

    private var inputStream: NSInputStream?
    private var outputStream: NSOutputStream?
    
    private var port: Int32
    
    private let responseLengthSize: Int32 = 4
    private let responseCorrelationIdSize: Int32 = 4

    private var _inputStreamQueue: dispatch_queue_t
    private var _outputStreamQueue: dispatch_queue_t
    private var _writeRequestBlocks = [dispatch_block_t]()
    
    init(ipv4: String, port: Int32, broker: Broker, clientId: String) {
        self.ipv4 = ipv4
        self.clientId = clientId
        self._broker = broker
        self.port = port

        _inputStreamQueue = dispatch_queue_create(
            "\(self.ipv4).\(self.port).input.stream.franz", DISPATCH_QUEUE_SERIAL
        )

        _outputStreamQueue = dispatch_queue_create(
            "\(self.ipv4).\(self.port).output.stream.franz", DISPATCH_QUEUE_SERIAL
        )

        super.init()

        CFStreamCreatePairWithSocketToHost(
            kCFAllocatorDefault,
            ipv4,
            UInt32(port),
            &readStream,
            &writeStream
        )

        inputStream = readStream?.takeUnretainedValue()
        outputStream = writeStream?.takeUnretainedValue()

        self.inputStream?.delegate = self
        self.inputStream?.scheduleInRunLoop(
            NSRunLoop.mainRunLoop(),
            forMode: NSDefaultRunLoopMode
        )

        self.outputStream?.delegate = self
        self.outputStream?.scheduleInRunLoop(
            NSRunLoop.mainRunLoop(),
            forMode: NSDefaultRunLoopMode
        )

        self.inputStream?.open()
        self.outputStream?.open()
    }
    
    private func read(timeout: Double = 3000) {
        //print("Read Block Added")
        dispatch_async(_inputStreamQueue) {
            //print("\tBeginning Input Stream Read")
            if let inputStream = self.inputStream {
                do {
                    let (size, correlationId) = try self.getMessageMetadata()
                    //print("\t\tReading Bytes")
                    var bytes = [UInt8]()
                    let startTime = NSDate().timeIntervalSince1970
                    while bytes.count < Int(size) {
                        
                        if inputStream.hasBytesAvailable {
                            var buffer = [UInt8](count: Int(size), repeatedValue: 0)
                            let bytesInBuffer = inputStream.read(&buffer, maxLength: Int(size))
                            //print("\t\tReading Bytes")
                            buffer = buffer.slice(0, length: bytesInBuffer)
                            //print("BUFFER(\(size)): \(buffer)")
                            bytes += buffer
                        }
                        
                        let currentTime = NSDate().timeIntervalSince1970
                        let timeDelta = (currentTime - startTime) * 1000

                        if  timeDelta >= timeout {
                            print("Timeout @ Delta \(timeDelta).")
                            break
                        }
                    }
                    
                    if let callback = self._requestCallbacks[correlationId] {
                        callback(bytes)
                    } else {
                        print(
                            "Unable to find reuqest callback for " +
                            "Correlation Id: \(correlationId)"
                        )
                    }
                } catch ConnectionError.ZeroLengthResponse {
                    print("Zero Lenth Response")
                } catch ConnectionError.PartialResponse(let size) {
                    print("Response Size: \(size) is invalid.")
                } catch ConnectionError.BytesNoLongerAvailable {
                    return
                } catch {
                    print("Error")
                }
            } else {
                print("Unable to find Input Stream")
            }

            //print("\tReleasing Input Stream Read")
        }
    }

    func write(request: KafkaRequest, callback: RequestCallback? = nil) {
        request.clientId = KafkaString(value: clientId)
        //print("Write Block Added")
        if let requestCallback = callback {
            _requestCallbacks[request.correlationId] = requestCallback
        }
        
        let dispatchBlock = dispatch_block_create(DISPATCH_BLOCK_INHERIT_QOS_CLASS) {
            //print("\tBeginning Output Stream Write")
            if let stream = self.outputStream {
                if stream.hasSpaceAvailable {
                    let data = request.data
                    //print("Data: \(data)")
                    let bytesPtr = data.bytes
                    let bytes = unsafeBitCast(bytesPtr, UnsafePointer<UInt8>.self)
                    //print("Writing to Output Stream: \(data.length)")
                    stream.write(bytes, maxLength: data.length)
                    //print("\tReleasing Output Stream Write(\(data.length))")
                } else {
                    print("No Space Available for Writing")
                }
            }
        }
        
        if let outputStream = outputStream {
            if outputStream.hasSpaceAvailable {
                dispatch_async(_outputStreamQueue, dispatchBlock)
            } else {
                _writeRequestBlocks.append(dispatchBlock)
            }
        } else {
            _writeRequestBlocks.append(dispatchBlock)
        }
    }
    
    private func getMessageMetadata() throws -> (Int32, Int32) {
        if let activeInputStream = inputStream {
            let length = responseLengthSize + responseCorrelationIdSize
            var buffer = Array<UInt8>(count: Int(length), repeatedValue: 0)
            
            if activeInputStream.hasBytesAvailable {
                activeInputStream.read(&buffer, maxLength: Int(length))
            } else {
                throw ConnectionError.BytesNoLongerAvailable
            }
            
            //print("SIZE BUFFER: \(buffer)")
            
            let sizeBytes = buffer.slice(0, length: Int(responseLengthSize))
            let responseLengthSizeInclusive = Int32(bytes: sizeBytes)
            
            if responseLengthSizeInclusive > length {
                return (
                    responseLengthSizeInclusive - responseLengthSize,
                    Int32(bytes: buffer.slice(0, length: Int(responseCorrelationIdSize)))
                )
            } else if responseLengthSizeInclusive == 0 {
                throw ConnectionError.ZeroLengthResponse
            } else {
                throw ConnectionError.PartialResponse(size: responseLengthSizeInclusive)
            }
        }
        
        throw ConnectionError.UnableToFindInputStream
    }
    
    func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
        //print("STREAM STATUS: \(aStream.streamStatus.description) => \(eventCode.description)")
        if let inputStream = aStream as? NSInputStream {
            let status = inputStream.streamStatus
            //print("INPUT STREAM STATUS: \(status.description) => \(eventCode.description)")
            switch status {
            case .Open:
                switch eventCode {
                case NSStreamEvent.HasBytesAvailable:
                    //print("Input Stream Has Bytes Available")
                    read()
                case NSStreamEvent.OpenCompleted:
                    return
                default:
                    print("STREAM EVENT: \(eventCode.description)")
                }
            case .Reading:
                return
            case .Error:
                print("INPUT STREAM ERROR: \(aStream.streamError?.description ?? String())")
                return
            default:
                print("INPUT STREAM STATUS: \(aStream.streamStatus.description)")
                return
            }
        } else if let outputStream = aStream as? NSOutputStream {
            let status = outputStream.streamStatus
            //print("OUTPUT STREAM STATUS: \(status.description) => \(eventCode.description)")
            switch status {
            case .Open:
                switch eventCode {
                case NSStreamEvent.HasSpaceAvailable:
                    //print("Requests: \(_writeRequestBlocks.count)")
                    if _writeRequestBlocks.count > 0 {
                        let block = _writeRequestBlocks.removeFirst()
                        dispatch_async(_outputStreamQueue, block)
                    }
                case NSStreamEvent.OpenCompleted:
                    return
                default:
                    print("OUTPUT STREAM EVENT: \(eventCode.description)")
                }
            case .Writing:
                return
            case .Error:
                print("OUTPUT STREAM ERROR: \(aStream.streamError?.description ?? String())")
                return
            default:
                print("OUTPUT STREAM STATUS:: \(aStream.streamStatus.description)")
                return
            }
        }
    }
    
    deinit {
        print("Deinitializing.")
    }
}