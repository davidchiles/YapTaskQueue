//
//  ViewController.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import UIKit

class MessageSendAction:NSObject, NSCoding, YapTaskQueueAction {
    let key:String
    let collection:String
    let messageToSendKey:String
    let messagetoSendCollection:String
    let queue:String
    let date:NSDate
    
    init(key:String, collection:String, messageToSendKey:String,messagetoSendCollection:String, queue:String, date:NSDate) {
        self.key = key
        self.collection = collection
        self.messageToSendKey = messageToSendKey
        self.messagetoSendCollection = messagetoSendCollection
        self.queue = queue
        self.date = date
    }
    
    //MARK: YapTaskQueueAction
    func yapKey() -> String {
        return self.key
    }
    
    func yapCollection() -> String {
        return self.collection
    }
    
    func queueName() -> String {
        return self.queue
    }
    
    func sort(otherObject: YapTaskQueueAction) -> NSComparisonResult {
        guard let otherAction = otherObject as? MessageSendAction else {
            return .OrderedSame
        }
        
        return self.date.compare(otherAction.date)
    }
    
    //MARK: NSCoding
    
    func encodeWithCoder(aCoder: NSCoder) {
        aCoder.encodeObject(self.key, forKey: "key")
        aCoder.encodeObject(self.collection, forKey: "collection")
        aCoder.encodeObject(self.messageToSendKey, forKey: "messageToSendKey")
        aCoder.encodeObject(self.messagetoSendCollection, forKey: "messagetoSendCollection")
        aCoder.encodeObject(self.queue, forKey: "queue")
        aCoder.encodeObject(self.date, forKey: "date")
    }
    
    required convenience init?(coder aDecoder: NSCoder) {
        guard let key = aDecoder.decodeObjectForKey("key") as? String,
        let collection = aDecoder.decodeObjectForKey("collection") as? String,
        let messageToSendKey = aDecoder.decodeObjectForKey("messageToSendKey") as? String,
        let messagetoSendCollection = aDecoder.decodeObjectForKey("messagetoSendCollection") as? String,
        let queue = aDecoder.decodeObjectForKey("queue") as? String,
        let date = aDecoder.decodeObjectForKey("date") as? NSDate
            else {
                return nil
        }
        
        self.init(key:key,collection: collection, messageToSendKey: messageToSendKey, messagetoSendCollection: messagetoSendCollection, queue: queue, date: date)
    }
}

class MessageHandler:YapTaskQueueHandler {
    
    @objc func handleNextItem(action: YapTaskQueueAction, completion: (success: Bool, retryTimeout: NSTimeInterval) -> Void) {
        guard let messageAction = action as? MessageSendAction else {
            completion(success: false, retryTimeout: -1)
            return
        }
        
        /**
         1. Get the 'real' message out of the database
         2. Send the message over the wire
         3. get result
        */
        
        let result = true
        /**
        If the sending was successful then return true and it doesn't matter what you set the `retryTimeout` to
        If the sedning was not successful then send bask false and when you want to retry
         It's also possible to set the retry timeout to -1 if you don't want a timed retry but would rather manually retry when the conditions are more likely to result in a success
        
        completion(success: result, retryTimeout: 5)
        */
        completion(success: result, retryTimeout: -1)
    }
}

class ViewController: UIViewController {

    override func viewDidLoad() {
        super.viewDidLoad()
        // Do any additional setup after loading the view, typically from a nib.
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }


}

