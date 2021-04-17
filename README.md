<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![Apache License 2.0 License][license-shield]][license-url]



<!-- PROJECT LOGO -->
<p align="center">
  <a href="https://github.com/mamaar/Liftbridge.Net">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>
</p>
  <h3 align="center">Liftbridge.Net </h3>

  <p>
  .NET client for <a href="https://github.com/liftbridge-io/liftbridge">Liftbridge</a>, a
system that provides lightweight, fault-tolerant message streams for
  <a href="https://nats.io">NATS</a>.
  </p>
<p>
Liftbridge provides the following high-level features:

- Log-based API for NATS
- Replicated for fault-tolerance
- Horizontally scalable
- Wildcard subscription support
- At-least-once delivery support and message replay
- Message key-value support
- Log compaction by key
</p>
<p>
  <a href="https://github.com/mamaar/Liftbridge.Net/issues">Report Bug</a>
  ·
  <a href="https://github.com/mamaar/Liftbridge.Net/issues">Request Feature</a>
</p>



<!-- TABLE OF CONTENTS -->
  <h2>Table of Contents</h2>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>



<!-- ABOUT THE PROJECT -->
## About The Project



<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites
A running [Liftbridge](https://github.com/liftbridge-io/liftbridge) cluster. For testing, the Liftbridge repository includes Docker configurations.

### Installation

#### NuGet

**This library is in early development and not published to NuGet. Development builds are based on *master* and deployed to [GitHub packages](https://github.com/mamaar/Liftbridge.Net/packages).**

#### Source

1. Clone the repo
   ```sh
   git clone https://github.com/mamaar/Liftbridge.Net.git
   ```
2. Install NPM packages
   ```sh
   dotnet build
   ```



<!-- USAGE EXAMPLES -->
## Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_



<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/mamaar/Liftbridge.Net/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the Apache License 2.0 License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Project Link: [https://github.com/mamaar/Liftbridge.Net](https://github.com/mamaar/Liftbridge.Net)



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements

* [Liftbridge](https://github.com/liftbridge-io/liftbridge)





<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/mamaar/Liftbridge.Net.svg?style=for-the-badge
[contributors-url]: https://github.com/mamaar/Liftbridge.Net/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/mamaar/Liftbridge.Net.svg?style=for-the-badge
[forks-url]: https://github.com/mamaar/Liftbridge.Net/network/members
[stars-shield]: https://img.shields.io/github/stars/mamaar/Liftbridge.Net.svg?style=for-the-badge
[stars-url]: https://github.com/mamaar/Liftbridge.Net/stargazers
[issues-shield]: https://img.shields.io/github/issues/mamaar/Liftbridge.Net.svg?style=for-the-badge
[issues-url]: https://github.com/mamaar/Liftbridge.Net/issues
[license-shield]: https://img.shields.io/github/license/mamaar/Liftbridge.Net.svg?style=for-the-badge
[license-url]: https://github.com/mamaar/Liftbridge.Net/blob/master/LICENSE.txt
