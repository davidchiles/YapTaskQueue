Pod::Spec.new do |s|


  s.name         = "YapTaskQueue"
  s.version      = "0.3.0"
  s.summary      = "A persistent serial queue based on YapDatabase"

  s.description  = <<-DESC
                      A persistent serial queue based on YapDatabase. This makes it possible to create a persistent queue of 'actions'
                      that can be dealt with between launches.
                   DESC

  s.homepage     = "https://github.com/davidchiles/YapTaskQueue"
 

   s.license      = { :type => "Public Domain", :file => "LICENSE" }



  s.author             = { "David Chiles" => "dwalterc@gmail.com" }

  s.platform     = :ios, "8.0"


  s.source       = { :git => "https://github.com/davidchiles/YapTaskQueue.git", :tag => s.version }

  # This kinda doesn't work because of https://github.com/yapstudios/YapDatabase/issues/308 but will work for chatsecure or until a new version of Yap is sent to trunk
  s.default_subspecs = 'Standard'
  s.subspec 'Standard' do |ss|
    ss.source_files  = "YapTaskQueue/Classes/**/*.swift"
    ss.dependency 'YapDatabase', '~> 3.0'
  end

  s.subspec 'SQLCipher' do |ss|
    ss.source_files  = "YapTaskQueue/Classes/**/*.swift"
    ss.dependency 'YapDatabase/SQLCipher', '~> 3.0'
  end

end
