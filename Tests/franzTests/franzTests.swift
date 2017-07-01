import XCTest
@testable import franz

class franzTests: XCTestCase {
    func testClusterClientId() {
		let cluster = Cluster(brokers: [("192.0.0.1", 9092)], clientId: "Test")
		
		XCTAssertEqual("Test", cluster.clientId)
    }


    static var allTests = [
        ("testClusterClientId", testClusterClientId),
    ]
}
