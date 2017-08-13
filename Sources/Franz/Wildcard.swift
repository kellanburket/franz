//
//  Wildcard.swift
//  KafkaClient
//
//  Created by Kellan Cummings on 1/10/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation

/**
 Wrapper class for NSRegularExpression with convenience methods for common string-parsing operations
 */
class RegExp {
    
    private var pattern: String = ""
    private var replacement: String = ""
    private var options: UInt = 0
    private var mOptions: UInt = 0
    private var regExp: NSRegularExpression?
    
    /**
     Initialize a new Regular Expression object with a pattern and options. The following flags are permitted:
     
     * i:    case-insenstive match
     * x:    ignore #-prefixed comments and whitespace in this pattern
     * s:    `.` matches `\n`
     * m:    `^`, `$` match the beginning and end of lines, respectively (set by default)
     * w:    use unicode word boundaries
     * c:    ignore metacharacters when matching (e.g, `\w`, `\d`, `\s`, etc..)
     * l:    use only `\n` as a line separator
     
     - parameter pattern: an ICU-style regular expression
     - parameter options: a string containing option flags
     
     */
    init(_ pattern: String, _ options: String = "") {
        setOptions("\(options)m")
        self.pattern = pattern
    }
    
    /**
     Counts the number of matches in a string
     
     - parameter input:   an input string
     
     - returns:    the number of matches in the input string
     */
    func count(_ input: String) -> Int? {
        let capacity = input.utf16.count
        
        if let regExp = doRegExp() {
            return regExp.numberOfMatches(
                in: input,
                options: NSRegularExpression.MatchingOptions(rawValue: mOptions),
                range: NSMakeRange(
                    0,
                    capacity
                )
            )
        }
        
        return nil
    }
    
    /**
     Looks for the first ICU-style pattern match in the input string
     
     - parameter input:   an input string
     
     - returns:    an array of matches or nil
     */
    func match(_ input: String) -> [String]? {
        var input = input
        input = input.replacingOccurrences(of: "\n", with: "\\n", options: NSString.CompareOptions.literal, range: nil)
        
        var matches: [String] = [String]()
        
        getFirstMatch(input) { result in
            
            let numRanges = result.numberOfRanges
            
            for i in 0..<numRanges {
				let range = result.range(at: i)
                let match = String(input[range.toStringIndexRange(input)])
                matches.append(match)
            }
        }
        
        switch matches.count {
        case 0: return nil
        default: return matches
        }
    }
    
    /**
     Looks for all ICU-style pattern matches in the input string
     
     - parameter input:   an input string
     
     - returns:    an array of an array of matches or nil
     */
    func scan(_ input: String) -> [[String]]? {
        var input = input
        input = input.replacingOccurrences(of: "\n", with: "\\n", options: NSString.CompareOptions.literal, range: nil)
        
        var matches: [[String]] = [[String]]()
        
        getMatches(input) { result, index in
            
            if matches.count - 1 < index {
                matches.append([String]())
            }
            
            let numRanges = result.numberOfRanges
            
            for i in 0..<numRanges {
				let range = result.range(at: i)
                let match = String(input[range.toStringIndexRange(input)])
                matches[index].append(match)
            }
        }
        
        switch matches.count {
        case 0: return nil
        default: return matches
        }
    }
    
    private func getAllMatches(_ input: String, reverse: Bool,  onMatch: (NSTextCheckingResult, Int) -> Void) {
        if let regExp = doRegExp() {
            var results = regExp.matches(
                in: input,
                options: NSRegularExpression.MatchingOptions(rawValue: mOptions),
                range: input.toRange()
            )
            
            if reverse {
                results = Array(results.reversed())
            }
            
            for (i, result) in results.enumerated() {
                onMatch(result, i)
            }
        }
    }
    
    private func getFirstMatch(_ input: String, onMatch: (NSTextCheckingResult) -> Void) {
        if let regExp = doRegExp() {
            
            let range = makeRange(input)
            
            var results = regExp.matches(
                in: input,
                options: NSRegularExpression.MatchingOptions(rawValue: mOptions),
                range: range
            )
            
            if results.count > 0 {
                onMatch(results[0])
            }
        }
    }
    
    private func getMatches(_ input: String, onMatch: (NSTextCheckingResult, Int) -> Void) {
        getAllMatches(input, reverse: false, onMatch: onMatch)
    }
    
    private func getReverseMatches(_ input: String, onMatch: (NSTextCheckingResult, Int) -> Void) {
        getAllMatches(input, reverse: true, onMatch: onMatch)
    }
    
    //Substitution
    internal func gsub(_ attributed: NSMutableAttributedString, _ replacement: String) -> NSMutableAttributedString {
        return NSMutableAttributedString(string: gsub(attributed.mutableString, replacement) as String)
    }
    
    /**
     Substitute all matches in input string with replacement string
     
     - parameter input:   an input string
     - parameter replacement: replacement string (supports back references)
     
     - returns:    the modified input string
     */
    func gsub(_ string: String, _ replacement: String) -> String {
        return gsub(string.toMutable(), replacement) as String
    }
    
    internal func gsub(_ mutable: NSMutableString, _ replacement: String) -> NSMutableString {
        self.replacement = replacement
        if let regExp = doRegExp() {
            regExp.replaceMatches(
                in: mutable,
                options: NSRegularExpression.MatchingOptions(rawValue: mOptions),
                range: NSMakeRange(0, mutable.length),
                withTemplate: self.replacement
            )
        }
        return mutable
    }
    
    /**
     Substitute all matches in input string with return value of callback function
     
     - parameter input:   an input string
     - parameter callback:    a callback function that takes a match as an argument and returns a modified string (does not support back references)
     
     - returns:    the modified input string
     */
    func gsub(_ string: String, callback: ((String) -> (String))) -> String {
        return gsub(string.toMutable(), callback: callback) as String
    }
    
    internal func gsub(_ mutable: NSMutableString, callback: ((String) -> (String))) -> NSMutableString {
        getReverseMatches(mutable as String) { result, index in
            let numRanges = result.numberOfRanges
            for i in 0..<numRanges {
				let range = result.range(at: i)
                let substring = mutable.substring(with: range)
                //println("Replacing: \(substring)")
                mutable.replaceCharacters(in: range, with: callback(substring))
            }
            
        }
        
        return mutable
    }
    
    /**
     Substitute the first matches in input string with replacement string
     
     - parameter input:   an input string
     - parameter replacement: replacement string (supports back references)
     
     - returns:    the modified input string
     */
    func sub(_ string: String, _ replacement: String) -> String {
        let mutable = string.toMutable()
        
        getFirstMatch(string) { result in
            if let regExp = self.regExp {
                
                let substitute = regExp.replacementString(
                    for: result,
                    in: string,
                    offset: 0,
                    template: replacement
                )
                
                mutable.replaceCharacters(
					in: result.range(at: 0),
                    with: substitute
                )
            }
        }
        
        return mutable as String
    }
    
    /* Utility functions for finding substring ranges */
    private func makeRange(_ input: String) -> NSRange {
        let capacity = input.utf16.count
        return NSMakeRange(0, capacity)
    }
    
    internal func getSubstringRanges(_ input: NSMutableAttributedString) -> [RegExpMatch]? {
        return getSubstringRanges(input.mutableString as String)
    }
    
    internal func getSubstringRanges(_ input: String) -> [RegExpMatch]? {
        var matches = [RegExpMatch]()
        
        getMatches(input) { result, index in
            let numRanges = result.numberOfRanges
			let matchRange = result.range(at: 0)
            let match = input.substringWithNSRange(matchRange)
            
            let regExpMatch: MatchTuple = (match, matchRange)
            var regExpSubmatches: [MatchTuple] = [MatchTuple]()
            
            for i in 1..<numRanges {
				let submatchRange = result.range(at: i)
                let submatch = input.substringWithNSRange(submatchRange)
                regExpSubmatches.append((submatch, submatchRange))
            }
            
            let nextMatch = RegExpMatch(
                pattern: self.pattern,
                match: regExpMatch,
                submatches: regExpSubmatches
            )
            
            matches.append(nextMatch)
        }
        
        if matches.count > 0 {
            return matches
        }
        
        return nil
    }
    
    ///TODO: Find out what these do and use them or don't
    private func setMatchingOptions(_ flags: String) -> UInt {
        /*
        NSMatchingOptions.ReportProgress
        NSMatchingOptions.ReportCompletion
        NSMatchingOptions.Anchored
        NSMatchingOptions.WithTransparentBounds
        NSMatchingOptions.WithoutAnchoringBounds
        */
        mOptions = UInt(0)
        return mOptions
    }
    
    private func setOptions(_ flags: String) {
        var options: UInt = 0
        
        for character in flags.characters {
            switch(character) {
            case("i"):
                options |= NSRegularExpression.Options.caseInsensitive.rawValue
            case("x"):
                options |= NSRegularExpression.Options.allowCommentsAndWhitespace.rawValue
            case("s"):
                options |= NSRegularExpression.Options.dotMatchesLineSeparators.rawValue
            case("m"):
                options |= NSRegularExpression.Options.anchorsMatchLines.rawValue
            case("w"):
                options |= NSRegularExpression.Options.useUnicodeWordBoundaries.rawValue
            case("c"):
                options |= NSRegularExpression.Options.ignoreMetacharacters.rawValue
            case("l"):
                options |= NSRegularExpression.Options.useUnixLineSeparators.rawValue
            default:
                options |= 0
            }
        }
        
        self.options = options
    }
    
	private func removeLinebreaks( _ input: inout String) {
		input = input.replacingOccurrences(of: "\r\n", with: "\n", options: [.literal], range: nil)
    }
    
    private func doRegExp() -> NSRegularExpression? {
        
        var error: NSError?
        
        do {
            regExp = try NSRegularExpression(
                pattern: pattern,
                options: NSRegularExpression.Options(rawValue: options)
            )
        } catch let error1 as NSError {
            error = error1
            regExp = nil
        }
        
        if let error = error {
			print("!!Error: There was an problem matching `\(pattern )`: \(error)")
            return nil
        } else {
            return regExp
        }
    }
}

internal typealias MatchTuple = (string: String, range: NSRange)

internal func ==(right: RegExpMatch, left: RegExpMatch) -> Bool {
    return right.match.range.location == left.match.range.location
        && right.match.range.length == left.match.range.length
}

internal class RegExpMatch: Equatable {
    var pattern: String
    var match: MatchTuple
    var submatches: [MatchTuple]
    var subexpressions = [RegExpMatch]()
    
    internal init(pattern: String, match: MatchTuple, submatches: [MatchTuple]) {
        self.pattern = pattern
        self.submatches = submatches
        self.match = match
    }
    
    internal var subrange: NSRange {
        get {
            return submatches[0].range
        }
        set(range) {
            submatches[0].range = range
        }
    }
    
    internal var substring: String {
        return submatches[0].string
    }
    
    internal var fullrange: NSRange {
        get {
            return match.range
        }
        
        set(range) {
            match.range = range
        }
    }
    
    internal var fullstring: String {
        return match.string
    }
    
    internal func addSubexpression(_ sub: RegExpMatch) {
        
        //println("\(sub.fullrange), \(sub.subrange): \(fullrange)")
        sub.fullrange = NSRange(
            location: sub.fullrange.location - fullrange.location,
            length: sub.fullrange.length
        )
        
        sub.subrange = NSRange(
            location: sub.subrange.location - fullrange.location,
            length: sub.subrange.length
        )

        subexpressions.append(sub)
    }
    
	internal func formatSubexpressions( _ replacement: inout NSMutableAttributedString) {
        if subexpressions.count > 0 {
            for sub in subexpressions {
                if let matches = RegExp(sub.pattern).getSubstringRanges(replacement) {
                    
                    for match in matches {
                        let substring = NSMutableAttributedString(
                            string: match.substring
                        )
                        
                        replacement.replaceCharacters(
                            in: match.fullrange,
                            with: substring
                        )
                    }
                }
            }
        }
    }
    
	internal class func nest( _ sets: inout [RegExpMatch]) {
        for setA in sets {
            for setB in sets {
                if setA != setB {
                    let intersection = NSIntersectionRange(setA.fullrange, setB.fullrange)
                    if intersection.location > 0 && intersection.length > 0 {
                        
                        if setA.fullrange.location <= setB.fullrange.location {
                            if let index = sets.index(of: setB) {
                                sets.remove(at: index)
                                setA.addSubexpression(setB)
                            }
                        } else {
                            if let index = sets.index(of: setA) {
                                sets.remove(at: index)
                                setB.addSubexpression(setA)
                            }
                        }
                    }
                }
            }
            
            if setA.subexpressions.count > 1 {
                RegExpMatch.nest(&setA.subexpressions)
            }
        }
        
        sets.sort {
            $0.fullrange.location > $1.fullrange.location
        }
    }
}

private let consonant = "[b-df-hj-np-tv-z]"
private let vowel = "[aeiou]"

let plurals: [(String, String)] = [
    ("(?<=f)oo(?=t)$|(?<=t)oo(?=th)$|(?<=g)oo(?=se)$", "ee"),
    ("(?<=i)fe$|(?<=[eao]l)f$|(?<=(l|sh)ea)f$", "ves"),
    ("(\\w{2,})[ie]x", "$1ices"),
    ("(?<=[ml])ouse$", "ice"),
    ("man$", "men"),
    ("child$", "children"),
    ("person$", "people"),
    ("eau$", "eaux"),
    ("(?<=-by)$", "s"),
    ("(?<=[^q]\(vowel)y)$", "s"),
    ("y$", "ies"),
    ("(?<=s|sh|tch)$", "es"),
    ("(?<=\(vowel)\(consonant)i)um", "a"),
    ("(?<=\\w)$", "s")
]

let singulars: [(String, String)] = [
    ("(?<=f)ee(?=t)$|(?<=t)ee(?=th)$|(?<=g)ee(?=se)$", "oo"),
    ("(?<=i)ves$", "fe"),
    ("(?<=[eao]l)ves$|(?<=(l|sh)ea)ves$", "f"),
    ("(?<=[ml])ice$", "ouse"),
    ("men$", "man"),
    ("children$", "child"),
    ("people$", "person"),
    ("eaux$", "eau"),
    ("(?<=-by)s$", ""),
    ("(?<=[^q]\(vowel)y)s$", ""),
    ("ies$", "y"),
    ("(?<=s|sh|tch)es$", ""),
    ("(?<=\(vowel)\(consonant)i)a", "um"),
    ("(?<=\\w)s$", "")
]

private let irregulars: [String:String] = [
    "potato": "potatoes",
    "di": "dice",
    "appendix": "appendices",
    "index": "indices",
    "matrix": "matrices",
    "radix": "radices",
    "vertex": "vertices",
    "radius": "radii",
    "goose": "geese"
]

infix operator =~ : AdditionPrecedence

/**
 Checks if the input matches the pattern
 
 - parameter left:   the input string
 - parameter right:    the pattern
 
 - returns:    returns true if pattern exists in the input string
 */
func =~(left: String, right: String) -> Bool {
    return left.match(right) != nil
}

extension String {
    
    /**
     Convert a string into an NSDate object.
     Currently supports both backslashes and hyphens in the following formats:
     
     * Y-m-d
     * m-d-Y
     * Y-n-j
     * n-j-Y
     
     - returns: a date
     */
    public func toDate() -> Date? {
        //println("to Date: \(self)")
        
        let patterns = [
            "\\w+ (\\w+) (\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2}) \\+\\d{4} (\\d{4})": [
                "month", "day", "hour", "minute", "second", "year"
            ],
            "(\\d{4})[-\\/](\\d{1,2})[-\\/](\\d{1,2})(?: (\\d{1,2}):(\\d{1,2}):(\\d{1,2}))?": [
                "year", "month", "day", "hour", "minute", "second"
            ],
            "(\\d{1,2})[-\\/](\\d{1,2})[-\\/](\\d{4})(?: (\\d{1,2}):(\\d{1,2}):(\\d{1,2}))?": [
                "month", "day", "year", "hour", "minute", "second"
            ]
        ]
        
        let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        
        for (pattern, map) in patterns {
            if let matches = self.match(pattern) {
                //println("Matches \(matches)")
                if(matches.count >= 4) {
                    var dictionary = [String:String]()
                    
                    for (i, item) in map.enumerated() {
                        if i + 1 < matches.count {
                            dictionary[item] = matches[i + 1]
                        } else {
                            break
                        }
                    }
                    
                    let calendar = Calendar.current
                    var comp = DateComponents()
                    
                    comp.year = 0
                    if let year_string = dictionary["year"],
						let year = Int(year_string)
                    {
                        comp.year = year
                    }
                    
                    comp.month = 0
                    if let month = dictionary["month"] {
                        if let month = Int(month) {
                            comp.month = month
                        } else {
                            for (i, m) in months.enumerated() {
                                if month =~ m {
                                    comp.month = i
                                    break
                                }
                            }
                        }
                    }
                    
                    comp.day = 0
					if let day_string = dictionary["day"], let day = Int(day_string) {
                        comp.day = day
                    }
                    
                    comp.hour = 0
					if let hour_string = dictionary["hour"], let hour = Int(hour_string) {
                        comp.hour = hour
                    }
                    
                    comp.minute = 0
					if let minute_string = dictionary["minute"], let minute = Int(minute_string) {
                        comp.minute = minute
                    }
                    
                    comp.second = 0
					if let second_string = dictionary["second"], let second = Int(second_string) {
                        comp.second = second
                    }
                    
                    return calendar.date(from: comp)
                }
            }
        }
        return nil
    }
    
    /**
     Split a string into an array of strings by slicing at delimiter
     
     - parameter delimiter:   character(s) to split string at
     
     - returns:  an array of strings if delimiter matches, or an array
     with the original string as its only component
     */
    func split(_ delimiter: String) -> [String] {
        let parsedDelimiter: String = NSRegularExpression.escapedPattern(for: delimiter)
        
        if let matches = self.scan("(.+?)(?:\(parsedDelimiter)|$)") {
            var arr = [String]()
            for match in matches {
                arr.append(match[1])
            }
            
            return arr
        } else {
            return [self]
        }
    }
    
    /**
     Substitute result of callback function for all occurences of pattern
     
     - parameter pattern: a regular expression string to match against
     - parameter callback:    a callback function to call on pattern match success
     
     - returns:    modified string
     */
    func gsub(_ pattern: String, callback: ((String) -> (String))) -> String {
        let regex = RegExp(pattern)
        return regex.gsub(self, callback: callback)
    }
    
    /**
     Substitute result of callback function for all occurences of pattern.
     The following flags are permitted:
     
     * i:    case-insenstive match
     * x:    ignore #-prefixed comments and whitespace in this pattern
     * s:    `.` matches `\n`
     * m:    `^`, `$` match the beginning and end of lines, respectively (set by default)
     * w:    use unicode word boundaries
     * c:    ignore metacharacters when matching (e.g, `\w`, `\d`, `\s`, etc..)
     * l:    use only `\n` as a line separator
     
     - parameter pattern: an ICU-style regular expression
     - parameter options: a string containing option flags
     - parameter callback:    a callback function to call on pattern match success
     
     - returns:    modified string
     */
    func gsub(_ pattern: String, options: String, callback: ((String) -> (String))) -> String {
        let regex = RegExp(pattern, options)
        return regex.gsub(self, callback: callback)
    }
    
    /**
     Convenience wrapper for gsub with options
     */
    func gsub(_ pattern: String, _ replacement: String, options: String = "") -> String {
        let regex = RegExp(pattern, options)
        return regex.gsub(self, replacement)
    }
    
    /**
     Convenience wrapper for case-insenstive gsub
     */
    func gsubi(_ pattern: String, _ replacement: String, options: String = "") -> String {
        let regex = RegExp(pattern,  "\(options)i")
        return regex.gsub(self, replacement)
    }
    
    /**
     Convenience wrapper for case-insensitive gsub with callback
     */
    func gsubi(_ pattern: String, callback: ((String) -> (String))) -> String {
        let regex = RegExp(pattern, "i")
        return regex.gsub(self, callback: callback)
    }
    
    /**
     Convenience wrapper for case-insensitive gsub with callback and options
     */
    func gsubi(_ pattern: String, options: String, callback: ((String) -> (String))) -> String {
        let regex = RegExp(pattern, "\(options)i")
        return regex.gsub(self, callback: callback)
    }
    
    
    /**
     Conveneience wrapper for first-match-only substitution
     */
    func sub(_ pattern: String, _ replacement: String, options: String = "") -> String {
        let regex = RegExp(pattern, options)
        return regex.sub(self, replacement)
    }
    
    /**
     Conveneience wrapper for case-insensitive first-match-only substitution
     */
    func subi(_ pattern: String, _ replacement: String, options: String = "") -> String {
        let regex = RegExp(pattern, "\(options)i")
        return regex.sub(self, replacement)
    }
    
    /**
     Scans and matches only the first pattern
     
     - parameter pattern: the pattern to search against
     - parameter   (not-required): options for matching--see documentation for `gsub`; defaults to ""
     
     - returns:    an array of all matches to the first pattern
     */
    func match(_ pattern: String, _ options: String = "") -> [String]? {
        return RegExp(pattern, options).match(self)
    }
    
    /**
     Scans and matches all patterns
     
     - parameter pattern: the pattern to search against
     - parameter   (not-required): options for matching--see documentation for `gsub`; defaults to ""
     
     - returns:    an array of arrays of each matched pattern
     */
    func scan(_ pattern: String, _ options: String = "") -> [[String]]? {
        return RegExp(pattern, options).scan(self)
    }
    
    /**
     Slices out the parts of the string that match the pattern
     
     - parameter pattern: the pattern to search against
     
     - returns:    an array of the slices
     */
    mutating func slice(_ pattern: String) -> [[String]]? {
        let matches = self.scan(pattern)
        self = self.gsub(pattern, "")
        return matches
    }
    
    /**
     Strip white space or aditional specified characters from beginning or end of string
     
     - parameter a: string of any characters additional characters to strip off beginning/end of string
     
     - returns: trimmed string
     */
    func trim(_ characters: String = "") -> String {
        let parsedCharacters = NSRegularExpression.escapedPattern(for: characters)
        return self.gsub("^[\\s\(parsedCharacters)]+|[\\s\(parsedCharacters)]+$", "")
    }
    
    /**
     Strip white space or aditional specified characters from end of string
     
     - parameter a: string of any characters additional characters to strip off end of string
     
     - returns: trimmed string
     */
    func rtrim(_ characters: String = "") -> String {
        let parsedCharacters = NSRegularExpression.escapedPattern(for: characters)
        return self.gsub("[\\s\(parsedCharacters)]+$", "")
    }
    
    /**
     Strip white space or aditional specified characters from beginning of string
     
     - parameter a: string of any characters additional characters to strip off beginning of string
     
     - returns: trimmed string
     */
    func ltrim(_ characters: String = "") -> String {
        let parsedCharacters = NSRegularExpression.escapedPattern(for: characters)
        return self.gsub("^[\\s\(parsedCharacters)]+", "")
    }
    
    /**
     Converts Html special characters (e.g. '&#169;' => '©')
     
     - returns:    converted string
     */
    func decodeHtmlSpecialCharacters() -> String {
        let regex = RegExp("&#[a-fA-F\\d]+;")
        
        return regex.gsub(self) { pattern in
            let hex = RegExp("[a-fA-F\\d]+")
            if let matches = hex.match(pattern) {
                if let sint = Int(matches[0]) {
                    let character = Character(UnicodeScalar(UInt32(sint))!)
                    return "\(character)"
                }
            }
            print("There was an issue while trying to decode character '\(pattern)'")
            return ""
        }
    }
    
    /**
     Converts a string to camelcase. e.g.: 'hello_world' -> 'HelloWorld'
     
     - returns:   a formatted string
     */
    func toCamelcase() -> String {
        return gsub("[_\\-\\s]\\w") { match in
            return match[match.characters.index(match.startIndex, offsetBy: 1)..<match.endIndex].uppercased()
        }
    }
    
    /**
     Converts a string to snakecase. e.g.: 'HelloWorld' -> 'hello_world'
     
     - parameter language: (Reserved for future use)
     
     - returns:   a formatted string
     */
    func toSnakecase() -> String {
        return gsub("[\\s-]\\w") { match in
            return "_" + match[match.characters.index(match.startIndex, offsetBy: 1)..<match.endIndex].lowercased()
            }.gsub("(?<!^)\\p{Lu}") { match in
                return "_\(match.lowercased())"
            }.lowercased()
    }
    
    /**
     DEVELOPMENTAL METHOD: Change String from singular to plural.
     
     - parameter language: (Reserved for future use)
     
     - returns:   a plural string
     */
    func pluralize(_ language: String = "en/us") -> String {
        if let plural = irregulars[self] {
            return plural
        }
        
        for (regex, mod) in plurals {
            let replacement = self.gsubi(regex, mod)
            if replacement != self {
                return replacement
            }
        }
        
        return self
    }
    
    /**
     DEVELOPMENTAL METHOD: Change String from plural to singular.
     
     - returns:   a singular string
     */
    func singularize(_ language: String = "en/us") -> String {
        if let plurals = irregulars.flip(), let plural = plurals[self] {
            return plural
        }
        
        for (regex, mod) in singulars {
            let replacement = self.gsubi(regex, mod)
            if replacement != self {
                return replacement
            }
        }
        
        return self
    }
    
    /**
     Set the first letter to lowercase
     
     - returns:   formatted string
     */
    func decapitalize() -> String {
        let prefix = self[startIndex..<characters.index(startIndex, offsetBy: 1)].lowercased()
        let body = self[characters.index(startIndex, offsetBy: 1)..<endIndex]
        return "\(prefix)\(body)"
    }
    
    /**
     Set the first letter to uppercase
     
     - returns:   formatted string
     */
    func capitalize() -> String {
        let prefix = self[startIndex..<characters.index(startIndex, offsetBy: 1)].uppercased()
        let body = self[characters.index(startIndex, offsetBy: 1)..<endIndex]
        return "\(prefix)\(body)"
    }
    
    /**
     Repeat String x times.
     
     - parameter the: number of times to repeat
     
     - returns:   formatted string
     */
    func `repeat`(_ times: Int) -> String {
        
        var rstring = ""
        if times > 0 {
            for _ in 0...times {
                rstring = "\(rstring)\(self)"
            }
        }
        return rstring
    }
    
    internal func substringWithNSRange(_ range: NSRange) -> String {
        return String(self[range.toStringIndexRange(self)])
    }
    
    internal func substringRanges(_ pattern: String, _ options: String = "") -> [RegExpMatch]? {
        return RegExp(pattern, options).getSubstringRanges(self)
    }
    
    internal func toMutable() -> NSMutableString {
        let capacity = self.utf16.count
        let mutable = NSMutableString(capacity: capacity)
        mutable.append(self)
        return mutable
    }
    
    internal func toRange() -> NSRange {
        let capacity = self.utf16.count
        return NSMakeRange(0, capacity)
    }
}

internal extension NSMutableString {
    internal func gsub(_ pattern: String, _ replacement: String) -> NSMutableString {
        let regex = RegExp(pattern)
        return regex.gsub(self, replacement)
    }
    
    internal func substringRanges(_ pattern: String, _ options: String = "") -> [RegExpMatch]? {
        return RegExp(pattern, options).getSubstringRanges(self as String)
    }
}

internal extension NSMutableAttributedString {
    internal func substringRanges(_ pattern: String, _ options: String = "") -> [RegExpMatch]? {
        return RegExp(pattern, options).getSubstringRanges(self)
    }
}

internal extension NSRange {
    internal func toStringIndexRange(_ input: String) -> Range<String.Index> {
        if location < input.utf16.count {
            let startIndex = input.characters.index(input.startIndex, offsetBy: location)
            let endIndex = input.characters.index(input.startIndex, offsetBy: location + length)
            let range = (startIndex ..< endIndex)
            //println(input.substringWithRange(range))
            return range
        }
        
        return (input.characters.indices.startIndex ..< input.characters.indices.endIndex)
    }
}

internal extension Dictionary {
    
    internal func flip() -> Dictionary<Key, Value>? {
        if Key.self is Value.Type {
            var out = Dictionary<Key, Value>()
            
            for key in self.keys {
				if let value = self[key] as? Key, let key = key as? Value {
                    out[value] = key
                }
            }
            
            return out.count > 0 ? out : nil
        }
        
        return nil
    }
}
