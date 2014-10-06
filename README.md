tectonica-commons
=================

Set of Java utility classes to provide lightwight solutions for common situations.  

All classes are completely independent from one another, and each resides in a single file.

To use in your project, add the following repository to your `pom.xml`:

	<repositories>
		<repository>
			<id>tectonica-commons</id>
			<url>https://raw.github.com/zach-m/tectonica-commons/mvn-repo/</url>
			<snapshots>
				<enabled>true</enabled>
				<updatePolicy>always</updatePolicy>
			</snapshots>
		</repository>
	</repositories>

and then you can simply add the dependency:
  
	<dependency>
		<groupId>com.tectonica</groupId>
		<artifactId>tectonica-commons</artifactId>
		<version>0.1.4</version>
	</dependency>

 