//
//  YapTaskQueueAction.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation

/// Implement this protocol in order for an action to enter a queue and eventually be passed to a YapTaskQueueHandler
public protocol YapTaskQueueAction {
    /// The yap key of this item
    func yapKey() -> String
    
    /// The yap collection of this item
    func yapCollection() -> String
    
    /// The queue that this item is in.
    func queueName() -> String
    
    /// How this item should be sorted compared to other items in it's queue
    func sort(otherObject:YapTaskQueueAction) -> NSComparisonResult
    
    /// If this action fails how long should it wait until it is retried. Careful if this on a synchronous queue then it will block up the whole queue
    //func retryTimeout() -> NSTimeInterval
}