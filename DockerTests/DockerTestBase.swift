//
//  DockerTestBase.swift
//  FranzTests
//
//  Created by Luke Lau on 21/08/2017.
//

import XCTest
import Foundation
@testable import Franz

class DockerTestBase: XCTestCase {
    
	static var docker: Process!
	static let startedSemaphore = DispatchSemaphore(value: 0)
	
	static let compose = URL(fileURLWithPath: "/usr/local/bin/docker-compose")
	static let jaas = Bundle(for: DockerTestBase.self).url(forResource: "kafka_server_jaas", withExtension: "conf")!
	
	class var yml: URL {
		return Bundle(for: DockerTestBase.self).url(forResource: "docker-compose", withExtension: "yml")!
	}
	
	class var host: String { return "localhost" }
	class var port: Int32 { return 9092 }
	
	static var dockerFolder: URL?
	
	override class func setUp() {
		if CommandLine.arguments.contains("--no-docker") {
			return
		}
		do {
			let downDocker = Process()
			downDocker.executableURL = compose
			downDocker.arguments = ["-f", yml.path, "down"]
			downDocker.standardOutput = nil
			try downDocker.run()
			downDocker.waitUntilExit()
			
			docker = Process()
			docker.executableURL = compose
			docker.arguments = ["-f", yml.path, "up"]
			docker.currentDirectoryURL = DockerTestBase.yml.deletingLastPathComponent()
			docker.standardOutput = nil
			
			try docker.run()
			waitForKafka()
		} catch {
			if let folder = dockerFolder {
				try? FileManager.default.removeItem(at: folder)
			}
			fatalError("Couldn't find docker-compose")
		}
	}
	
	class func waitForKafka() {
		let delegate = DockerStreamDelegate()
		var inputStream: InputStream?, outputStream: OutputStream?
		var timer: Timer?
		
		var runLoopToStop: CFRunLoop!
		
		DispatchQueue(label: "FranzDockerStreamPoll").async {
			let cancelTimer = Timer.scheduledTimer(withTimeInterval: 60, repeats: false) { _ in
				timer?.invalidate()
				tearDown()
				fatalError("Couldn't connect to Kafka")
			}
			
			timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { _ in
				
				if !delegate.retry {
					cancelTimer.invalidate()
					return
				}
				
				delegate.written = false
				delegate.retry = false
				
				inputStream?.close()
				outputStream?.close()
				
				Stream.getStreamsToHost(withName: host, port: Int(port), inputStream: &inputStream, outputStream: &outputStream)
				
				inputStream!.delegate = delegate
				outputStream!.delegate = delegate
				
				delegate.inputStream = inputStream
				delegate.outputStream = outputStream
				
				inputStream!.schedule(in: .current, forMode: .commonModes)
				outputStream!.schedule(in: .current, forMode: .commonModes)
				
				inputStream!.open()
				outputStream!.open()
			}
			runLoopToStop = CFRunLoopGetCurrent()
			CFRunLoopRun()
		}
		startedSemaphore.wait()
		
		CFRunLoopStop(runLoopToStop)
		
		print("Kafka server is ready")
		
		inputStream?.close()
		outputStream?.close()
		
		timer?.invalidate()
	}
	
	class DockerStreamDelegate: NSObject, StreamDelegate {
		
		var inputStream: InputStream!, outputStream: OutputStream!
		var retry = true
		var written = false
		
		func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
			if eventCode == .errorOccurred {
				print(aStream.streamError?.localizedDescription ?? "")
				retry = true
			}
			if aStream == inputStream, eventCode == .hasBytesAvailable {
				
				//Attempt to read in the size of the list groups request
				var data = Data(capacity: 8)
				data.withUnsafeMutableBytes { (bytes: UnsafeMutablePointer<UInt8>) -> Void in
					
					inputStream.read(bytes, maxLength: 8)
					var readData = Data(buffer: UnsafeBufferPointer(start: bytes, count: 8))
					let responseSize = UInt32(data: &readData)
					let correlationId = UInt32(data: &readData)
					
					//If it's a zero-length response, retry
					if responseSize > 0, correlationId == 1234 {
						startedSemaphore.signal()
					} else {
						retry = true
					}
				}
			}
			if aStream == outputStream, eventCode == .hasSpaceAvailable, !written {
				print("Trying to contact Kafka server")
				let req = ApiVersionsRequest()
				let data = req.data(correlationId: 1234, clientId: "test")
					
				data.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) -> Void in
					outputStream.write(bytes, maxLength: data.count)
				}
				
				written = true
			}
		}
		
	}
	
	override class func tearDown() {
		do {
			try Process.run(compose, arguments: ["-f", yml.path, "stop"]).waitUntilExit()
			if let folder = dockerFolder {
				try FileManager.default.removeItem(at: folder)
			}
		} catch {
			print("Failed to stop containers")
		}
	}
    
}
