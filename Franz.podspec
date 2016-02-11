#
# Be sure to run `pod lib lint Franz.podspec' to ensure this is a
# valid spec before submitting.
#
# Any lines starting with a # are optional, but their use is encouraged
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html
#

Pod::Spec.new do |s|
  s.name             = "Franz"
  s.version          = "0.1.0"
  s.summary          = "An Apache Kafka Client for iOS and OSX."

# This description is used to generate tags and improve search results.
#   * Think: What does it do? Why did you write it? What is the focus?
#   * Try to keep it short, snappy and to the point.
#   * Write the description between the DESC delimiters below.
#   * Finally, don't worry about the indent, CocoaPods strips it!  
  s.description      = <<-DESC
Franz is an Apache Kafka 0.9.0 client for iOS and OSx. Franz supports both simple and high-level consumers.
                       DESC

  s.homepage         = "https://github.com/kellanburket/franz"
  s.license          = 'MIT'
  s.author           = { "kellanburket" => "kellan.burket@gmail.com" }
  s.source           = { :git => "https://github.com/kellanburket/franz.git", :tag => s.version.to_s }

  s.requires_arc = true

  s.source_files = 'Pod/Classes/**/*'
  s.resource_bundles = {
    'Franz' => ['Pod/Assets/*.png']
  }

  s.platforms = { :ios => '9.0', :osx => '10.0' }
  use_frameworks!
end
