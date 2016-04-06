//
//  YapTaskQueueAction.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation

public protocol YapTaskQueueAction {
    func yapKey() -> String
    func yapCollection() -> String
    func queueName() -> String?
    func sort(otherObject:YapTaskQueueAction) -> NSComparisonResult
}