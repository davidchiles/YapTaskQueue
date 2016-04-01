//
//  YapTaskQueueTests.swift
//  YapTaskQueueTests
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import XCTest
import YapDatabase
@testable import YapTaskQueue

class TestActionObject:NSObject, YapTaskQueueAction, NSCoding {
    var name = "name"
    var queue = "default"
    var date = NSDate()
    
    override init() {
        
    }
    
    func sort(otherObject: YapTaskQueueAction) -> NSComparisonResult {
        guard let otherAction = otherObject as? TestActionObject else {
            return .OrderedSame
        }
        return self.date.compare(otherAction.date)
    }
    
    func action() -> YapAction? {
        return { [weak self] in
            sleep(1)
            print("\(self!.name) - \(self!.date)")
            NSNotificationCenter.defaultCenter().postNotification(NSNotification(name: "notification", object: self))
            return true
        }

    }
    
    func queueName() -> String? {
        return self.queue
    }
    
    ///NSCoding
    internal func encodeWithCoder(aCoder: NSCoder) {
        aCoder.encodeObject(self.name, forKey: "name")
        aCoder.encodeObject(self.queue, forKey: "queue")
        aCoder.encodeObject(self.date, forKey: "date")
    }
    
    internal required init?(coder aDecoder: NSCoder) {
        self.name = aDecoder.decodeObjectForKey("name") as! String
        self.queue = aDecoder.decodeObjectForKey("queue") as! String
        self.date = aDecoder.decodeObjectForKey("date") as! NSDate
    }
}

class YapTaskQueueTests: XCTestCase {
    
    var database:YapDatabase? = nil
    
    override func setUp() {
        super.setUp()
        
        let path = NSTemporaryDirectory().stringByAppendingString("/db.sqlite")
        if (NSFileManager.defaultManager().fileExistsAtPath(path)) {
            try! NSFileManager.defaultManager().removeItemAtPath(path)
        }
        
        // Setup datbase
        self.database = YapDatabase(path: path)
        
        // Setup Extension
        let options = YapDatabaseViewOptions()
        let ext = YapTaskQueueExtension(options: options)
        self.database?.registerExtension(ext, withName: "extName")
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testOneAction() {
        
        let expectation = self.expectationWithDescription("test one action")
        
        let token = NSNotificationCenter.defaultCenter().addObserverForName("notification", object: nil, queue: NSOperationQueue.currentQueue()) { (note) in
            expectation.fulfill()
        }
        
        self.database?.newConnection().readWriteWithBlock({ (transaction) in
            let action = TestActionObject()
            transaction .setObject(action, forKey: "key", inCollection: "collection")
        })
        
        self.waitForExpectationsWithTimeout(100) { (error) in
            NSNotificationCenter.defaultCenter().removeObserver(token)
        }
    }
    
}
